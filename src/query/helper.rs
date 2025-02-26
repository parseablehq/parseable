// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Helper that helps with interactive editing, including multi-line parsing and validation,
//! and auto-completion for file name during creating external table.

use std::borrow::Cow;

use super::highlighter::{NoSyntaxHighlighter, SyntaxHighlighter};

use datafusion::common::sql_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use datafusion::sql::sqlparser::parser::ParserError;

use rustyline::completion::{Completer, FilenameCompleter, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper, Result};

pub struct CliHelper {
    completer: FilenameCompleter,
    dialect: String,
    highlighter: Box<dyn Highlighter>,
}

impl CliHelper {
    pub fn new(dialect: &str, color: bool) -> Self {
        let highlighter: Box<dyn Highlighter> = if !color {
            Box::new(NoSyntaxHighlighter {})
        } else {
            Box::new(SyntaxHighlighter::new(dialect))
        };
        Self {
            completer: FilenameCompleter::new(),
            dialect: dialect.into(),
            highlighter,
        }
    }

    fn validate_input(&self, input: &str) -> Result<ValidationResult> {
        if let Some(sql) = input.strip_suffix(';') {
            let sql = match unescape_input(sql) {
                Ok(sql) => sql,
                Err(err) => {
                    return Ok(ValidationResult::Invalid(Some(format!(
                        "  🤔 Invalid statement: {err}",
                    ))))
                }
            };

            let dialect = match dialect_from_str(&self.dialect) {
                Some(dialect) => dialect,
                None => {
                    return Ok(ValidationResult::Invalid(Some(format!(
                        "  🤔 Invalid dialect: {}",
                        self.dialect
                    ))))
                }
            };
            let lines = split_from_semicolon(sql);
            for line in lines {
                match DFParser::parse_sql_with_dialect(&line, dialect.as_ref()) {
                    Ok(statements) if statements.is_empty() => {
                        return Ok(ValidationResult::Invalid(Some(
                            "  🤔 You entered an empty statement".to_string(),
                        )));
                    }
                    Ok(_statements) => {}
                    Err(err) => {
                        return Ok(ValidationResult::Invalid(Some(format!(
                            "  🤔 Invalid statement: {err}",
                        ))));
                    }
                }
            }
            Ok(ValidationResult::Valid(None))
        } else if input.starts_with('\\') {
            // command
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }
}

impl Default for CliHelper {
    fn default() -> Self {
        Self::new("generic", false)
    }
}

impl Highlighter for CliHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize, kind: CmdKind) -> bool {
        self.highlighter.highlight_char(line, pos, kind)
    }
}

impl Hinter for CliHelper {
    type Hint = String;
}

/// returns true if the current position is after the open quote for
/// creating an external table.
fn is_open_quote_for_location(line: &str, pos: usize) -> bool {
    let mut sql = line[..pos].to_string();
    sql.push('\'');
    if let Ok(stmts) = DFParser::parse_sql(&sql) {
        if let Some(Statement::CreateExternalTable(_)) = stmts.back() {
            return true;
        }
    }
    false
}

impl Completer for CliHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> std::result::Result<(usize, Vec<Pair>), ReadlineError> {
        if is_open_quote_for_location(line, pos) {
            self.completer.complete(line, pos, ctx)
        } else {
            Ok((0, Vec::with_capacity(0)))
        }
    }
}

impl Validator for CliHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        let input = ctx.input().trim_end();
        self.validate_input(input)
    }
}

impl Helper for CliHelper {}

/// Unescape input string from readline.
///
/// The data read from stdio will be escaped, so we need to unescape the input before executing the input
pub fn unescape_input(input: &str) -> datafusion::error::Result<String> {
    let mut chars = input.chars();

    let mut result = String::with_capacity(input.len());
    while let Some(char) = chars.next() {
        if char == '\\' {
            if let Some(next_char) = chars.next() {
                // https://static.rust-lang.org/doc/master/reference.html#literals
                result.push(match next_char {
                    '0' => '\0',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    '\\' => '\\',
                    _ => {
                        return Err(sql_datafusion_err!(ParserError::TokenizerError(format!(
                            "unsupported escape char: '\\{}'",
                            next_char
                        ))))
                    }
                });
            }
        } else {
            result.push(char);
        }
    }

    Ok(result)
}

/// Splits a string which consists of multiple queries.
pub(crate) fn split_from_semicolon(sql: String) -> Vec<String> {
    let mut commands = Vec::new();
    let mut current_command = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for c in sql.chars() {
        if c == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if c == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        }

        if c == ';' && !in_single_quote && !in_double_quote {
            if !current_command.trim().is_empty() {
                commands.push(format!("{};", current_command.trim()));
                current_command.clear();
            }
        } else {
            current_command.push(c);
        }
    }

    if !current_command.trim().is_empty() {
        commands.push(format!("{};", current_command.trim()));
    }

    commands
}
