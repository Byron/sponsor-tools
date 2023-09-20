//! A tailor-made match engine to be able to auto-apply notes to matching rows.
//! sle = simple logic engine

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Statement {
    pub value_column_index: usize,
    pub operation: Operation,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Operation {
    Equals,
    EndsWith,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Rule {
    /// All statements have to be true for a match
    pub statements: Vec<Statement>,
    /// The value to apply if the rule matches.
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Engine {
    pub rules: Vec<Rule>,
}

impl Statement {
    pub fn matches(&self, record: &csv::ByteRecord) -> Option<()> {
        let value = record.get(self.value_column_index)?;
        match self.operation {
            Operation::Equals => {
                if !value.eq(self.value.as_bytes()) {
                    return None;
                }
            }
            Operation::EndsWith => {
                if !value.ends_with(self.value.as_bytes()) {
                    return None;
                }
            }
        }
        Some(())
    }
}

impl Rule {
    pub fn matches(&self, record: &csv::ByteRecord) -> bool {
        self.statements
            .iter()
            .all(|stm| stm.matches(record).is_some())
    }
}

impl Engine {
    pub fn matching_rule(&self, record: &csv::ByteRecord) -> Option<&Rule> {
        self.rules
            .iter()
            .find_map(|rule| rule.matches(record).then_some(rule))
    }
}
