mod arena;
mod catalog;
mod executor;
mod hnsw;
mod memtable;
mod parser;
mod segment;
mod sequence;
mod storage;
mod types;
mod wal;
mod window;

use rustler::{Encoder, Env, Term};

/// Boot the WAL subsystem from environment variables. Extracted from
/// `on_load` so cargo tests can exercise the same path the NIF loader
/// takes, since `on_load` itself needs an `Env` and can only run
/// inside the BEAM.
///
/// Env contract:
/// - `EVOLVSQL_WAL_ENABLED=1` turns the WAL on (default: off)
/// - `EVOLVSQL_WAL_PATH=/some/file.wal` picks the log file
///
/// On enable, we replay any existing WAL before accepting queries so
/// recovered state is visible immediately. A recovery failure is
/// logged to stderr and leaves the WAL disabled rather than aborting
/// NIF load — a broken WAL file must not brick the whole engine.
pub(crate) fn boot_wal_from_env() {
    match wal::manager::enable_from_env() {
        Ok(true) => {
            if let Err(e) = wal::recovery::recover() {
                eprintln!("evolvsql: WAL recovery failed, disabling WAL: {}", e);
                wal::manager::disable();
            }
        }
        Ok(false) => {}
        Err(e) => {
            eprintln!("evolvsql: WAL enable failed, running in-memory: {}", e);
        }
    }
}

/// Rustler load callback. Runs once when the NIF library is loaded by
/// the BEAM (on `:engine.start` or first `Engine.Native` call). This
/// is the only place we can enable the WAL for the running process;
/// without it `enable_from_env` would only ever run in tests, and the
/// production engine would silently start up with no WAL at all.
fn on_load(_env: Env, _info: Term) -> bool {
    boot_wal_from_env();
    true
}

#[rustler::nif]
fn ping() -> &'static str {
    "pong from rust engine"
}

#[rustler::nif(schedule = "DirtyCpu")]
fn parse_sql(sql: &str) -> Result<String, String> {
    parser::parse(sql)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn parse_sql_ast(sql: &str) -> Result<String, String> {
    parser::parse_ast(sql)
}

/// Execute SQL and return result as a native Erlang term.
/// Returns {:ok, %{tag: str, columns: [[name, oid], ...], rows: [[val, ...], ...]}}
/// or {:error, message}
#[rustler::nif(schedule = "DirtyCpu")]
fn execute_sql<'a>(env: Env<'a>, sql: &str) -> Term<'a> {
    match executor::execute(sql) {
        Ok(result) => {
            let tag = result.tag.encode(env);
            let columns: Vec<Term<'a>> = result
                .columns
                .iter()
                .map(|(name, oid)| {
                    let pair: (Term, Term) = (name.as_str().encode(env), oid.encode(env));
                    pair.encode(env)
                })
                .collect();
            let rows: Vec<Term<'a>> = result
                .rows
                .iter()
                .map(|row| {
                    let vals: Vec<Term<'a>> = row
                        .iter()
                        .map(|v| match v {
                            Some(s) => s.as_str().encode(env),
                            None => rustler::types::atom::nil().encode(env),
                        })
                        .collect();
                    vals.encode(env)
                })
                .collect();

            let map = rustler::Term::map_new(env);
            let map = map.map_put(
                rustler::types::atom::Atom::from_str(env, "tag").unwrap().encode(env),
                tag,
            ).unwrap();
            let map = map.map_put(
                rustler::types::atom::Atom::from_str(env, "columns").unwrap().encode(env),
                columns.encode(env),
            ).unwrap();
            let map = map.map_put(
                rustler::types::atom::Atom::from_str(env, "rows").unwrap().encode(env),
                rows.encode(env),
            ).unwrap();

            (rustler::types::atom::ok(), map).encode(env)
        }
        Err(msg) => {
            (rustler::types::atom::error(), msg.as_str()).encode(env)
        }
    }
}

rustler::init!("Elixir.Engine.Native", load = on_load);
