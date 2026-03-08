use std::{error::Error, fmt};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccelError {
    pub code: String,
    pub message: String,
    pub operator: Option<String>,
    pub retryable: bool,
}

pub type AccelResult<T> = Result<T, AccelError>;

impl AccelError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            operator: None,
            retryable: false,
        }
    }

    pub fn with_operator(mut self, operator: impl Into<String>) -> Self {
        self.operator = Some(operator.into());
        self
    }

    pub fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }
}

impl fmt::Display for AccelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for AccelError {}

impl From<String> for AccelError {
    fn from(value: String) -> Self {
        Self::new("internal", value)
    }
}

impl From<&str> for AccelError {
    fn from(value: &str) -> Self {
        Self::new("internal", value)
    }
}

#[cfg(test)]
mod tests {
    use super::AccelError;

    #[test]
    fn display_uses_message_only() {
        let err = AccelError::new("bad_input", "workflow step must be object")
            .with_operator("workflow_run")
            .retryable(false);
        assert_eq!(err.to_string(), "workflow step must be object");
        assert_eq!(err.code, "bad_input");
        assert_eq!(err.operator.as_deref(), Some("workflow_run"));
        assert!(!err.retryable);
    }
}
