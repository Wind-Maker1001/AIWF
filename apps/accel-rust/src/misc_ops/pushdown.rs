use super::*;

pub(crate) fn run_aggregate_pushdown_v1(
    req: AggregatePushdownReq,
) -> Result<AggregatePushdownResp, String> {
    if req.group_by.is_empty() {
        return Err("group_by is empty".to_string());
    }
    let from = req.from.as_deref().unwrap_or("data").trim().to_string();
    if from.is_empty() {
        return Err("from is empty".to_string());
    }
    let from = validate_sql_identifier(&from)?;
    let group_by = req
        .group_by
        .iter()
        .map(|g| validate_sql_identifier(g))
        .collect::<Result<Vec<_>, String>>()?;
    let specs = parse_agg_specs(&req.aggregates)?;
    let select_group = group_by.join(", ");
    let select_aggs = specs
        .iter()
        .map(|s| match s.op.as_str() {
            "count" => Ok(format!(
                "COUNT(1) AS {}",
                validate_sql_identifier(&s.as_name)?
            )),
            "sum" => Ok(format!(
                "SUM({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "avg" => Ok(format!(
                "AVG({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "min" => Ok(format!(
                "MIN({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "max" => Ok(format!(
                "MAX({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            _ => Err("unsupported aggregate op".to_string()),
        })
        .collect::<Result<Vec<_>, String>>()?
        .join(", ");
    let where_sql = req
        .where_sql
        .as_deref()
        .map(validate_where_clause)
        .transpose()?
        .map(|w| format!(" WHERE {w}"))
        .unwrap_or_default();
    let limit = req.limit.unwrap_or(10000).max(1);
    let sql = format!(
        "SELECT {select_group}, {select_aggs} FROM {from}{where_sql} GROUP BY {select_group}"
    );
    let rows = match req.source_type.to_lowercase().as_str() {
        "sqlite" => load_sqlite_rows(&req.source, &sql, limit)?,
        "sqlserver" => load_sqlserver_rows(&req.source, &sql, limit)?,
        _ => return Err("source_type must be sqlite or sqlserver".to_string()),
    };
    Ok(AggregatePushdownResp {
        ok: true,
        operator: "aggregate_pushdown_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        sql,
        stats: json!({"rows": rows.len(), "limit": limit, "source_type": req.source_type}),
        rows,
    })
}
