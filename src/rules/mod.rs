use crate::storage::{Datarow, Datavalue};
use chrono::Utc;
use serde_json::Value;
use std::fmt;

pub const RULES_LOG_NAME: &str = "rules";

pub const RULE_LOG_NAME_COLUMN_HEADER: &str = "log_name";
pub const RULE_KEY_COLUMN_HEADER: &str = "key";
pub const RULE_CONDITION_COLUMN_HEADER: &str = "condition";
pub const RULE_VALUE_COLUMN_HEADER: &str = "value";
pub const RULE_ACTION_COLUMN_HEADER: &str = "action";

pub const RULES_KEYS: [&str; 5] = [
    RULE_LOG_NAME_COLUMN_HEADER,
    RULE_KEY_COLUMN_HEADER,
    RULE_CONDITION_COLUMN_HEADER,
    RULE_VALUE_COLUMN_HEADER,
    RULE_ACTION_COLUMN_HEADER,
];
pub const LESS_CONDITION: &str = "<";
pub const GREATER_CONDITION: &str = ">";
pub const IS_CONDITION: &str = "is";
pub const IS_NOT_CONDITION: &str = "is not";
pub const CONTAINS_CONDITION: &str = "contains";
pub const NOT_CONTAINS_CONDITION: &str = "not contains";

pub const INFO_ACTION: &str = "info";
pub const WARN_ACTION: &str = "warn";
pub const ERROR_ACTION: &str = "error";
pub const SKIP_ACTION: &str = "skip further rules";

#[derive(Debug)]
pub(crate) enum RuleCondition {
    Less,
    Greater,
    Is,
    IsNot,
    Contains,
    NotContains,
}

impl fmt::Display for RuleCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RuleCondition::*;
        match self {
            Less => write!(f, "{}", LESS_CONDITION),
            Greater => write!(f, "{}", GREATER_CONDITION),
            Is => write!(f, "{}", IS_CONDITION),
            IsNot => write!(f, "{}", IS_NOT_CONDITION),
            Contains => write!(f, "{}", CONTAINS_CONDITION),
            NotContains => write!(f, "{}", NOT_CONTAINS_CONDITION),
        }
    }
}

impl<'a> TryFrom<&'a str> for RuleCondition {
    type Error = &'static str;

    fn try_from(condition: &'a str) -> Result<Self, &'static str> {
        match condition {
            LESS_CONDITION => Ok(Self::Less),
            GREATER_CONDITION => Ok(Self::Greater),
            IS_CONDITION => Ok(Self::Is),
            IS_NOT_CONDITION => Ok(Self::IsNot),
            CONTAINS_CONDITION => Ok(Self::Contains),
            NOT_CONTAINS_CONDITION => Ok(Self::NotContains),
            _ => Err("invalid condition"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Action {
    Info,
    Warn,
    Error,
    SkipFurtherRules,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Action::*;
        match self {
            Info => write!(f, "{}", INFO_ACTION),
            Warn => write!(f, "{}", WARN_ACTION),
            Error => write!(f, "{}", ERROR_ACTION),
            SkipFurtherRules => write!(f, "{}", SKIP_ACTION),
        }
    }
}

impl<'a> TryFrom<&'a str> for Action {
    type Error = &'static str;

    fn try_from(action: &'a str) -> Result<Self, &'static str> {
        match action {
            INFO_ACTION => Ok(Self::Info),
            WARN_ACTION => Ok(Self::Warn),
            ERROR_ACTION => Ok(Self::Error),
            SKIP_ACTION => Ok(Self::SkipFurtherRules),
            _ => Err("invalid action"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Rule {
    pub(crate) log_name: String,
    pub(crate) key: String,
    pub(crate) condition: RuleCondition,
    pub(crate) value: Datavalue,
    pub(crate) action: Action,
}

impl Rule {
    pub(crate) fn try_from_values(row: Vec<Value>) -> Option<Self> {
        if row.len() != 6 {
            // Rule fields + timestamp which is first
            return None;
        }
        let log_name = row[1].as_str()?.trim().to_string();
        let key = row[2].as_str()?.trim().to_string();
        let condition = RuleCondition::try_from(row[3].as_str()?).ok()?;
        let value = &row[4];
        let action = Action::try_from(row[5].as_str()?).ok()?;

        let value = match condition {
            RuleCondition::Is if value.is_boolean() => Datavalue::Bool(value.as_bool()?),
            RuleCondition::Is if value.is_u64() => Datavalue::Integer(value.as_u64()?),
            RuleCondition::IsNot if value.is_boolean() => Datavalue::Bool(value.as_bool()?),
            RuleCondition::IsNot if value.is_u64() => Datavalue::Integer(value.as_u64()?),
            RuleCondition::Less if value.is_number() => Datavalue::Number(value.as_f64()?),
            RuleCondition::Greater if value.is_number() => Datavalue::Number(value.as_f64()?),
            RuleCondition::Contains if value.is_string() => {
                Datavalue::Text(value.as_str()?.to_string())
            }
            RuleCondition::NotContains if value.is_string() => {
                Datavalue::Text(value.as_str()?.to_string())
            }
            _ => {
                return None;
            }
        };

        Some(Self {
            log_name,
            key,
            condition,
            value,
            action,
        })
    }
}

// to add some example rules
impl Into<Datarow> for Rule {
    fn into(self) -> Datarow {
        let Self {
            log_name,
            key,
            condition,
            value,
            action,
        } = self;
        Datarow::new(
            RULES_LOG_NAME.to_string(),
            Utc::now().naive_utc(),
            vec![
                (
                    RULE_LOG_NAME_COLUMN_HEADER.to_string(),
                    Datavalue::Text(log_name),
                ),
                (RULE_KEY_COLUMN_HEADER.to_string(), Datavalue::Text(key)),
                (
                    RULE_CONDITION_COLUMN_HEADER.to_string(),
                    Datavalue::Text(condition.to_string()),
                ),
                (RULE_VALUE_COLUMN_HEADER.to_string(), value),
                (
                    RULE_ACTION_COLUMN_HEADER.to_string(),
                    Datavalue::Text(action.to_string()),
                ),
            ],
            None,
        )
    }
}
