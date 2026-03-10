pub(crate) fn validate_sql_identifier(s: &str) -> Result<String, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty sql identifier".to_string());
    }
    let ok = t
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.');
    if !ok || t.starts_with('.') || t.ends_with('.') || t.contains("..") {
        return Err(format!("invalid sql identifier: {s}"));
    }
    Ok(t.to_string())
}

pub(crate) fn validate_where_clause(s: &str) -> Result<String, String> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(String::new());
    }
    // Strict mode: only allow conjunction/disjunction of simple predicates:
    // identifier op literal, where op in (=, !=, >, >=, <, <=, like)
    let lower = t.to_lowercase().replace('\n', " ");
    let tokens = lower
        .split_whitespace()
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    if tokens.len() < 3 {
        return Err("where_sql too short".to_string());
    }
    let ident_ok = |x: &str| {
        !x.is_empty()
            && x.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    };
    let op_ok = |x: &str| matches!(x, "=" | "!=" | ">" | ">=" | "<" | "<=" | "like");
    let lit_ok = |x: &str| {
        if x.starts_with('\'') && x.ends_with('\'') && x.len() >= 2 {
            return true;
        }
        x.parse::<f64>().is_ok()
    };
    let mut i = 0usize;
    while i < tokens.len() {
        if i + 2 >= tokens.len() {
            return Err("where_sql invalid predicate tail".to_string());
        }
        if !ident_ok(tokens[i]) || !op_ok(tokens[i + 1]) || !lit_ok(tokens[i + 2]) {
            return Err("where_sql contains unsupported predicate".to_string());
        }
        i += 3;
        if i >= tokens.len() {
            break;
        }
        if !matches!(tokens[i], "and" | "or") {
            return Err("where_sql only supports AND/OR connectors".to_string());
        }
        i += 1;
        if i >= tokens.len() {
            return Err("where_sql ends with connector".to_string());
        }
    }
    Ok(t.to_string())
}

pub(crate) fn validate_readonly_query(query: &str) -> Result<String, String> {
    let q = query.trim();
    if q.is_empty() {
        return Err("query is empty".to_string());
    }
    if q.contains(';') {
        return Err("query contains ';' which is not allowed".to_string());
    }
    let lower = q.to_lowercase();
    if !lower.starts_with("select ") {
        return Err("only SELECT query is allowed".to_string());
    }
    let banned = [
        " insert ",
        " update ",
        " delete ",
        " drop ",
        " alter ",
        " create ",
        " truncate ",
        " exec ",
        " execute ",
        " attach ",
        " pragma ",
    ];
    if banned.iter().any(|kw| lower.contains(kw)) {
        return Err("query contains forbidden keyword".to_string());
    }
    Ok(q.to_string())
}
