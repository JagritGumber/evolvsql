use base64::{Engine as B64Engine, engine::general_purpose::STANDARD as B64};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpStream;

type HmacSha256 = Hmac<Sha256>;

pub struct Connection {
    stream: TcpStream,
}

pub struct QueryResult {
    pub tag: String,
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<Option<String>>>,
}

pub struct ColumnInfo {
    pub name: String,
    #[allow(dead_code)]
    pub type_oid: i32,
}

impl Connection {
    pub fn connect(
        host: &str,
        port: u16,
        user: &str,
        dbname: &str,
        password: &str,
    ) -> Result<Self, String> {
        let addr = format!("{}:{}", host, port);
        let mut stream =
            TcpStream::connect(&addr).map_err(|e| format!("connect to {}: {}", addr, e))?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .ok();

        let mut params = Vec::new();
        params.extend_from_slice(b"user\0");
        params.extend_from_slice(user.as_bytes());
        params.push(0);
        params.extend_from_slice(b"database\0");
        params.extend_from_slice(dbname.as_bytes());
        params.push(0);
        params.push(0);

        let len = (4 + 4 + params.len()) as u32;
        let mut msg = Vec::new();
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&3u16.to_be_bytes());
        msg.extend_from_slice(&0u16.to_be_bytes());
        msg.extend_from_slice(&params);
        stream.write_all(&msg).map_err(|e| e.to_string())?;

        let mut conn = Self { stream };

        loop {
            let (tag, payload) = conn.read_msg()?;
            match tag {
                b'R' => {
                    if payload.len() >= 4 {
                        let auth =
                            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                        match auth {
                            0 => {} // AuthenticationOk
                            3 if !password.is_empty() => {
                                let mut pw = password.as_bytes().to_vec();
                                pw.push(0);
                                conn.send_msg(b'p', &pw)?;
                            }
                            10 => {
                                // SASL — SCRAM-SHA-256
                                conn.scram_auth(user, password, &payload[4..])?;
                            }
                            _ => return Err(format!("unsupported auth method: {}", auth)),
                        }
                    }
                }
                b'Z' => break,
                b'E' => return Err(parse_error(&payload)),
                _ => {}
            }
        }

        Ok(conn)
    }

    fn scram_auth(&mut self, user: &str, password: &str, mechanisms: &[u8]) -> Result<(), String> {
        // Check for SCRAM-SHA-256
        let mechs = String::from_utf8_lossy(mechanisms);
        if !mechs.contains("SCRAM-SHA-256") {
            return Err("server requires unsupported SASL mechanism".into());
        }

        // Client-first message
        let nonce: String = (0..24)
            .map(|_| {
                let idx = rand::random::<u8>() % 62;
                match idx {
                    0..=25 => (b'A' + idx) as char,
                    26..=51 => (b'a' + idx - 26) as char,
                    _ => (b'0' + idx - 52) as char,
                }
            })
            .collect();
        let client_first_bare = format!("n={},r={}", user, nonce);
        let client_first = format!("n,,{}", client_first_bare);

        // SASLInitialResponse
        let mechanism = b"SCRAM-SHA-256\0";
        let cf_bytes = client_first.as_bytes();
        let mut payload = Vec::new();
        payload.extend_from_slice(mechanism);
        payload.extend_from_slice(&(cf_bytes.len() as u32).to_be_bytes());
        payload.extend_from_slice(cf_bytes);
        self.send_msg(b'p', &payload)?;

        // Read server-first message (AuthenticationSASLContinue, R with auth=11)
        let (tag, resp) = self.read_msg()?;
        if tag != b'R' {
            return Err("expected SASL continue".into());
        }
        let server_first = String::from_utf8_lossy(&resp[4..]).to_string();

        // Parse server-first: r=<nonce>,s=<salt>,i=<iterations>
        let mut server_nonce = String::new();
        let mut salt_b64 = String::new();
        let mut iterations = 4096u32;
        for part in server_first.split(',') {
            if let Some(v) = part.strip_prefix("r=") {
                server_nonce = v.to_string();
            } else if let Some(v) = part.strip_prefix("s=") {
                salt_b64 = v.to_string();
            } else if let Some(v) = part.strip_prefix("i=") {
                iterations = v.parse().unwrap_or(4096);
            }
        }

        if !server_nonce.starts_with(&nonce) {
            return Err("server nonce doesn't match".into());
        }

        let salt = B64.decode(&salt_b64).map_err(|e| e.to_string())?;

        // Derive keys
        let mut salted_password = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted_password);

        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");

        // Client-final message (without proof)
        let channel_binding = B64.encode(b"n,,");
        let client_final_without_proof =
            format!("c={},r={}", channel_binding, server_nonce);

        // Auth message
        let auth_message = format!(
            "{},{},{}",
            client_first_bare, server_first, client_final_without_proof
        );

        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        let client_final = format!("{},p={}", client_final_without_proof, B64.encode(&proof));

        // SASLResponse
        self.send_msg(b'p', client_final.as_bytes())?;

        // Read server-final (AuthenticationSASLFinal, R with auth=12)
        let (tag, resp) = self.read_msg()?;
        if tag == b'E' {
            return Err(parse_error(&resp));
        }
        if tag == b'R' && resp.len() >= 4 {
            let auth = u32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]);
            if auth == 12 {
                // Verify server signature
                let server_final = String::from_utf8_lossy(&resp[4..]).to_string();
                let expected_sig = hmac_sha256(&server_key, auth_message.as_bytes());
                let expected_b64 = B64.encode(&expected_sig);
                if let Some(v) = server_final.strip_prefix("v=") {
                    if v != expected_b64 {
                        return Err("server signature mismatch".into());
                    }
                }
            }
        }

        // Read AuthenticationOk
        let (tag, _) = self.read_msg()?;
        if tag != b'R' {
            return Err("expected AuthenticationOk after SASL".into());
        }

        Ok(())
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult, String> {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        self.send_msg(b'Q', &payload)?;

        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut tag = String::new();

        loop {
            let (msg_tag, data) = self.read_msg()?;
            match msg_tag {
                b'T' => columns = parse_row_desc(&data),
                b'D' => rows.push(parse_data_row(&data)),
                b'C' => tag = parse_command_complete(&data),
                b'Z' => break,
                b'E' => {
                    let err = parse_error(&data);
                    loop {
                        let (t, _) = self.read_msg()?;
                        if t == b'Z' {
                            break;
                        }
                    }
                    return Err(err);
                }
                _ => {}
            }
        }

        Ok(QueryResult { tag, columns, rows })
    }

    pub fn close(&mut self) {
        let _ = self.send_msg(b'X', &[]);
    }

    fn send_msg(&mut self, tag: u8, payload: &[u8]) -> Result<(), String> {
        let len = (4 + payload.len()) as u32;
        let mut msg = vec![tag];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(payload);
        self.stream.write_all(&msg).map_err(|e| e.to_string())
    }

    fn read_msg(&mut self) -> Result<(u8, Vec<u8>), String> {
        let mut hdr = [0u8; 5];
        self.stream
            .read_exact(&mut hdr)
            .map_err(|e| format!("read: {}", e))?;
        let tag = hdr[0];
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let payload_len = len.saturating_sub(4);
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            self.stream
                .read_exact(&mut payload)
                .map_err(|e| format!("read: {}", e))?;
        }
        Ok((tag, payload))
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    Sha256::digest(data).to_vec()
}

fn parse_row_desc(data: &[u8]) -> Vec<ColumnInfo> {
    let ncols = u16::from_be_bytes([data[0], data[1]]) as usize;
    let mut cols = Vec::with_capacity(ncols);
    let mut off = 2;
    for _ in 0..ncols {
        let nul = data[off..].iter().position(|&b| b == 0).unwrap_or(0);
        let name = String::from_utf8_lossy(&data[off..off + nul]).to_string();
        off += nul + 1;
        let type_oid = if off + 10 <= data.len() {
            i32::from_be_bytes([data[off + 6], data[off + 7], data[off + 8], data[off + 9]])
        } else {
            0
        };
        off += 18;
        cols.push(ColumnInfo { name, type_oid });
    }
    cols
}

fn parse_data_row(data: &[u8]) -> Vec<Option<String>> {
    let ncols = u16::from_be_bytes([data[0], data[1]]) as usize;
    let mut vals = Vec::with_capacity(ncols);
    let mut off = 2;
    for _ in 0..ncols {
        let len = i32::from_be_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
        off += 4;
        if len < 0 {
            vals.push(None);
        } else {
            let s = String::from_utf8_lossy(&data[off..off + len as usize]).to_string();
            off += len as usize;
            vals.push(Some(s));
        }
    }
    vals
}

fn parse_command_complete(data: &[u8]) -> String {
    let nul = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    String::from_utf8_lossy(&data[..nul]).to_string()
}

fn parse_error(data: &[u8]) -> String {
    let mut msg = String::new();
    let mut off = 0;
    while off < data.len() && data[off] != 0 {
        let field_type = data[off];
        off += 1;
        let nul = data[off..].iter().position(|&b| b == 0).unwrap_or(0);
        let value = String::from_utf8_lossy(&data[off..off + nul]).to_string();
        off += nul + 1;
        if field_type == b'M' {
            msg = value;
        }
    }
    msg
}
