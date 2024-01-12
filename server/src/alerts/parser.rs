/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::{borrow::Cow, str::FromStr};

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{cut, map, value},
    error::{convert_error, VerboseError},
    sequence::{delimited, preceded, separated_pair},
    IResult as NomIResult, Parser,
};

use super::rule::{
    base::{
        ops::{NumericOperator, StringOperator},
        NumericRule, StringRule,
    },
    CompositeRule,
};

type IResult<'a, O> = NomIResult<&'a str, O, VerboseError<&'a str>>;

enum StrFragment<'a> {
    Escaped(char),
    Unescaped(&'a str),
}

fn parse_escaped_char(input: &str) -> IResult<char> {
    preceded(
        char('\\'),
        alt((
            value('"', char('"')),
            value('\\', char('\\')),
            value('/', char('/')),
            value('\n', char('n')),
            value('\r', char('r')),
            value('\t', char('t')),
            value('\u{08}', char('b')),
            value('\u{0C}', char('f')),
        )),
    )
    .parse(input)
}

fn parse_str_char(input: &str) -> IResult<StrFragment> {
    alt((
        map(parse_escaped_char, StrFragment::Escaped),
        map(is_not(r#""\"#), StrFragment::Unescaped),
    ))
    .parse(input)
}

fn parse_string(input: &str) -> IResult<Cow<str>> {
    let mut res = Cow::Borrowed("");
    let (mut input, _) = char('"').parse(input)?;

    loop {
        match char('"').parse(input) {
            // If it is terminating double quotes then we can return the ok value
            Ok((tail, _)) => return Ok((tail, res)),
            // Fail to parsing in recoverable variant can mean it is a valid char that is not double quote
            Err(nom::Err::Error(_)) => {}
            Err(err) => return Err(err),
        };

        input = match cut(parse_str_char)(input)? {
            (tail, StrFragment::Escaped(ch)) => {
                res.to_mut().push(ch);
                tail
            }
            (tail, StrFragment::Unescaped(s)) => {
                if res.is_empty() {
                    res = Cow::Borrowed(s)
                } else {
                    res.to_mut().push_str(s)
                }
                tail
            }
        };
    }
}

fn parse_numeric_op(input: &str) -> IResult<NumericOperator> {
    alt((
        map(tag("<="), |_| NumericOperator::LessThanEquals),
        map(tag(">="), |_| NumericOperator::GreaterThanEquals),
        map(tag("!="), |_| NumericOperator::NotEqualTo),
        map(tag("<"), |_| NumericOperator::LessThan),
        map(tag(">"), |_| NumericOperator::GreaterThan),
        map(tag("="), |_| NumericOperator::EqualTo),
    ))(input)
}

fn parse_string_op(input: &str) -> IResult<StringOperator> {
    alt((
        map(tag("!="), |_| StringOperator::NotExact),
        map(tag("=%"), |_| StringOperator::Contains),
        map(tag("!%"), |_| StringOperator::NotContains),
        map(tag("="), |_| StringOperator::Exact),
        map(tag("~"), |_| StringOperator::Regex),
    ))(input)
}

fn parse_numeric_rule(input: &str) -> IResult<CompositeRule> {
    let (remaining, key) = map(parse_identifier, |s: &str| s.to_string())(input)?;
    let (remaining, op) = delimited(multispace0, parse_numeric_op, multispace0)(remaining)?;
    let (remaining, value) = map(take_while1(|c: char| c.is_ascii_digit()), |x| {
        str::parse(x).unwrap()
    })(remaining)?;

    Ok((
        remaining,
        CompositeRule::Numeric(NumericRule {
            column: key,
            operator: op,
            value,
        }),
    ))
}

fn parse_string_rule(input: &str) -> IResult<CompositeRule> {
    let (remaining, key) = map(parse_identifier, |s: &str| s.to_string())(input)?;
    let (remaining, op) = delimited(multispace0, parse_string_op, multispace0)(remaining)?;
    let (remaining, value) = parse_string(remaining)?;

    Ok((
        remaining,
        CompositeRule::String(StringRule {
            column: key,
            operator: op,
            value: value.into_owned(),
            ignore_case: None,
        }),
    ))
}

fn parse_identifier(input: &str) -> IResult<&str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '-' || c == '_')(input)
}

fn parse_unary_expr(input: &str) -> IResult<CompositeRule> {
    map(
        delimited(tag("!("), cut(parse_expression), char(')')),
        |x| CompositeRule::Not(Box::new(x)),
    )(input)
}

fn parse_bracket_expr(input: &str) -> IResult<CompositeRule> {
    delimited(
        char('('),
        delimited(multispace0, cut(parse_expression), multispace0),
        cut(char(')')),
    )(input)
}

fn parse_and(input: &str) -> IResult<CompositeRule> {
    let (remaining, (lhs, rhs)) = separated_pair(
        parse_atom,
        delimited(multispace1, tag("and"), multispace1),
        cut(parse_term),
    )(input)?;

    Ok((remaining, CompositeRule::And(vec![lhs, rhs])))
}

fn parse_or(input: &str) -> IResult<CompositeRule> {
    let (remaining, (lhs, rhs)) = separated_pair(
        parse_term,
        delimited(multispace1, tag("or"), multispace1),
        cut(parse_expression),
    )(input)?;

    Ok((remaining, CompositeRule::Or(vec![lhs, rhs])))
}

fn parse_expression(input: &str) -> IResult<CompositeRule> {
    alt((parse_or, parse_term))(input)
}

fn parse_term(input: &str) -> IResult<CompositeRule> {
    alt((parse_and, parse_atom))(input)
}

fn parse_atom(input: &str) -> IResult<CompositeRule> {
    alt((
        alt((parse_string_rule, parse_numeric_rule)),
        parse_unary_expr,
        parse_bracket_expr,
    ))(input)
}

impl FromStr for CompositeRule {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let (remaining, parsed) = parse_expression(s).map_err(|err| match err {
            nom::Err::Incomplete(_) => "Needed more data".to_string(),
            nom::Err::Error(err) | nom::Err::Failure(err) => convert_error(s, err),
        })?;

        if remaining.is_empty() {
            Ok(parsed)
        } else {
            Err(format!("Could not parse input \n{}", remaining))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::alerts::rule::{
        base::{
            ops::{NumericOperator, StringOperator},
            NumericRule, StringRule,
        },
        CompositeRule,
    };

    #[test]
    fn test_and_or_not() {
        let input = r#"key=500 and key="value" or !(key=300)"#;
        let rule = CompositeRule::from_str(input).unwrap();

        let numeric1 = NumericRule {
            column: "key".to_string(),
            operator: NumericOperator::EqualTo,
            value: serde_json::Number::from(500),
        };

        let string1 = StringRule {
            column: "key".to_string(),
            operator: StringOperator::Exact,
            value: "value".to_string(),
            ignore_case: None,
        };

        let numeric3 = NumericRule {
            column: "key".to_string(),
            operator: NumericOperator::EqualTo,
            value: serde_json::Number::from(300),
        };

        assert_eq!(
            rule,
            CompositeRule::Or(vec![
                CompositeRule::And(vec![
                    CompositeRule::Numeric(numeric1),
                    CompositeRule::String(string1)
                ]),
                CompositeRule::Not(Box::new(CompositeRule::Numeric(numeric3)))
            ])
        )
    }

    #[test]
    fn test_complex() {
        let input = r#"(verb =% "list" or verb =% "get") and (resource = "secret" and username !% "admin")"#;
        let rule = CompositeRule::from_str(input).unwrap();

        let verb_like_list = StringRule {
            column: "verb".to_string(),
            operator: StringOperator::Contains,
            value: "list".to_string(),
            ignore_case: None,
        };

        let verb_like_get = StringRule {
            column: "verb".to_string(),
            operator: StringOperator::Contains,
            value: "get".to_string(),
            ignore_case: None,
        };

        let resource_exact_secret = StringRule {
            column: "resource".to_string(),
            operator: StringOperator::Exact,
            value: "secret".to_string(),
            ignore_case: None,
        };

        let username_notcontains_admin = StringRule {
            column: "username".to_string(),
            operator: StringOperator::NotContains,
            value: "admin".to_string(),
            ignore_case: None,
        };

        assert_eq!(
            rule,
            CompositeRule::And(vec![
                CompositeRule::Or(vec![
                    CompositeRule::String(verb_like_list),
                    CompositeRule::String(verb_like_get)
                ]),
                CompositeRule::And(vec![
                    CompositeRule::String(resource_exact_secret),
                    CompositeRule::String(username_notcontains_admin)
                ]),
            ])
        )
    }
}
