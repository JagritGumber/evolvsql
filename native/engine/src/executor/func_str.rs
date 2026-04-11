use crate::arena::{ArenaValue, QueryArena};

/// Evaluate string functions: substring, trim, strpos, replace, left, right, split_part.
pub(crate) fn eval_string_func(name: &str, args: &[ArenaValue], arena: &mut QueryArena) -> Result<ArenaValue, String> {
    match name {
        "substring" | "substr" => {
            if args.is_empty() { return Err("substring() requires at least 2 arguments".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let chars: Vec<char> = s.chars().collect();
            let from = match args.get(1) {
                Some(ArenaValue::Int(n)) => (*n as usize).saturating_sub(1),
                _ => 0,
            };
            let len = match args.get(2) {
                Some(ArenaValue::Int(n)) => *n as usize,
                _ => chars.len().saturating_sub(from),
            };
            let result: String = chars.iter().skip(from).take(len).collect();
            Ok(ArenaValue::Text(arena.alloc_str(&result)))
        }
        "btrim" | "trim" => {
            if args.is_empty() { return Err("trim() requires at least 1 argument".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let chars_to_trim = match args.get(1) {
                Some(ArenaValue::Text(t)) => arena.get_str(*t).to_string(),
                _ => " ".to_string(),
            };
            let trimmed = s.trim_matches(|c: char| chars_to_trim.contains(c)).to_string();
            Ok(ArenaValue::Text(arena.alloc_str(&trimmed)))
        }
        "ltrim" => {
            if args.is_empty() { return Err("ltrim() requires at least 1 argument".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let ct = match args.get(1) { Some(ArenaValue::Text(t)) => arena.get_str(*t).to_string(), _ => " ".to_string() };
            Ok(ArenaValue::Text(arena.alloc_str(&s.trim_start_matches(|c: char| ct.contains(c)).to_string())))
        }
        "rtrim" => {
            if args.is_empty() { return Err("rtrim() requires at least 1 argument".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let ct = match args.get(1) { Some(ArenaValue::Text(t)) => arena.get_str(*t).to_string(), _ => " ".to_string() };
            Ok(ArenaValue::Text(arena.alloc_str(&s.trim_end_matches(|c: char| ct.contains(c)).to_string())))
        }
        "strpos" | "position" => {
            if args.len() != 2 { return Err("strpos() requires 2 arguments".into()); }
            if args[0].is_null() || args[1].is_null() { return Ok(ArenaValue::Null); }
            let haystack = args[0].to_text(arena).unwrap_or_default();
            let needle = args[1].to_text(arena).unwrap_or_default();
            Ok(ArenaValue::Int(haystack.find(&needle).map(|p| p + 1).unwrap_or(0) as i64))
        }
        "replace" => {
            if args.len() != 3 { return Err("replace() requires 3 arguments".into()); }
            if args.iter().any(|a| a.is_null()) { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let from = args[1].to_text(arena).unwrap_or_default();
            let to = args[2].to_text(arena).unwrap_or_default();
            Ok(ArenaValue::Text(arena.alloc_str(&s.replace(&from, &to))))
        }
        "left" => {
            if args.len() != 2 { return Err("left() requires 2 arguments".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let n = match &args[1] { ArenaValue::Int(i) => *i as usize, _ => return Err("left() requires integer second argument".into()) };
            Ok(ArenaValue::Text(arena.alloc_str(&s.chars().take(n).collect::<String>())))
        }
        "right" => {
            if args.len() != 2 { return Err("right() requires 2 arguments".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let n = match &args[1] { ArenaValue::Int(i) => *i as usize, _ => return Err("right() requires integer second argument".into()) };
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(ArenaValue::Text(arena.alloc_str(&chars[start..].iter().collect::<String>())))
        }
        _ => Err(format!("unknown string function: {}", name)),
    }
}
