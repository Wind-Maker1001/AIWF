use regex::Regex;

#[derive(Clone)]
pub(crate) enum FilterOp {
    Exists,
    NotExists,
    Eq(String),
    Ne(String),
    Contains(String),
    In(Vec<String>),
    NotIn(Vec<String>),
    Regex(Regex),
    NotRegex(Regex),
    Gt(f64),
    Gte(f64),
    Lt(f64),
    Lte(f64),
    BlankRow,
    SubtotalRow(Vec<String>),
    HeaderRepeatRow {
        header_values: Vec<String>,
        min_matches: usize,
    },
    NoteRow(Vec<String>),
    Invalid,
    Passthrough,
}

#[derive(Clone)]
pub(crate) struct CompiledFilter {
    pub(crate) field: String,
    pub(crate) op: FilterOp,
}
