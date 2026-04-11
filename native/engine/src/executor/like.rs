/// SQL LIKE pattern matching. `%` matches any sequence, `_` matches one char.
/// Backslash escapes the next character (e.g., `\%` matches literal `%`).
pub(crate) fn sql_like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let text_owned;
    let pattern_owned;
    let (text, pattern) = if case_insensitive {
        text_owned = text.to_lowercase();
        pattern_owned = pattern.to_lowercase();
        (text_owned.as_str(), pattern_owned.as_str())
    } else {
        (text, pattern)
    };

    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (tlen, plen) = (t.len(), p.len());

    let mut dp = vec![vec![false; plen + 1]; tlen + 1];
    dp[0][0] = true;

    for j in 1..=plen {
        if p[j - 1] == '%' { dp[0][j] = dp[0][j - 1]; } else { break; }
    }

    for i in 1..=tlen {
        let mut j = 0;
        while j < plen {
            let pj = j;
            let pc = if p[pj] == '\\' && pj + 1 < plen {
                j += 1;
                (p[j], true)
            } else {
                (p[pj], false)
            };

            if pc.1 {
                dp[i][j + 1] = dp[i - 1][pj] && t[i - 1] == pc.0;
            } else if pc.0 == '%' {
                dp[i][j + 1] = dp[i][j] || dp[i - 1][j + 1];
            } else if pc.0 == '_' {
                dp[i][j + 1] = dp[i - 1][j];
            } else {
                dp[i][j + 1] = dp[i - 1][j] && t[i - 1] == pc.0;
            }
            j += 1;
        }
    }
    dp[tlen][plen]
}
