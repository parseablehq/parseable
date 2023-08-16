use std::str::FromStr;

use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::map,
    sequence::{delimited, separated_pair},
    IResult,
};

use super::rule::{
    base::{
        ops::{NumericOperator, StringOperator},
        NumericRule, StringRule,
    },
    CompositeRule,
};

fn parse_numeric_op(input: &str) -> IResult<&str, NumericOperator> {
    alt((
        map(tag("<="), |_| NumericOperator::LessThanEquals),
        map(tag(">="), |_| NumericOperator::GreaterThanEquals),
        map(tag("!="), |_| NumericOperator::NotEqualTo),
        map(tag("<"), |_| NumericOperator::LessThan),
        map(tag(">"), |_| NumericOperator::GreaterThan),
        map(tag("="), |_| NumericOperator::EqualTo),
    ))(input)
}

fn parse_string_op(input: &str) -> IResult<&str, StringOperator> {
    alt((
        map(tag("!="), |_| StringOperator::NotExact),
        map(tag("=%"), |_| StringOperator::Contains),
        map(tag("!%"), |_| StringOperator::NotContains),
        map(tag("="), |_| StringOperator::Exact),
        map(tag("~"), |_| StringOperator::Regex),
    ))(input)
}

fn parse_numeric_rule(input: &str) -> IResult<&str, CompositeRule> {
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

fn parse_string_rule(input: &str) -> IResult<&str, CompositeRule> {
    let (remaining, key) = map(parse_identifier, |s: &str| s.to_string())(input)?;
    let (remaining, op) = delimited(multispace0, parse_string_op, multispace0)(remaining)?;
    let (remaining, value) = map(
        delimited(char('"'), take_until("\""), char('"')),
        |x: &str| x.to_string(),
    )(remaining)?;

    Ok((
        remaining,
        CompositeRule::String(StringRule {
            column: key,
            operator: op,
            value,
            ignore_case: None,
        }),
    ))
}

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '-' || c == '_')(input)
}

fn parse_unary_expr(input: &str) -> IResult<&str, CompositeRule> {
    map(delimited(tag("!("), parse_expression, char(')')), |x| {
        CompositeRule::Not(Box::new(x))
    })(input)
}

fn parse_bracket_expr(input: &str) -> IResult<&str, CompositeRule> {
    delimited(
        char('('),
        delimited(multispace0, parse_expression, multispace0),
        char(')'),
    )(input)
}

fn parse_and(input: &str) -> IResult<&str, CompositeRule> {
    let (remaining, (lhs, rhs)) = separated_pair(
        parse_atom,
        delimited(multispace1, tag("and"), multispace1),
        parse_term,
    )(input)?;

    Ok((remaining, CompositeRule::And(vec![lhs, rhs])))
}

fn parse_or(input: &str) -> IResult<&str, CompositeRule> {
    let (remaining, (lhs, rhs)) = separated_pair(
        parse_term,
        delimited(multispace1, tag("or"), multispace1),
        parse_expression,
    )(input)?;

    Ok((remaining, CompositeRule::Or(vec![lhs, rhs])))
}

fn parse_expression(input: &str) -> IResult<&str, CompositeRule> {
    alt((parse_or, parse_term))(input)
}
fn parse_term(input: &str) -> IResult<&str, CompositeRule> {
    alt((parse_and, parse_atom))(input)
}
fn parse_atom(input: &str) -> IResult<&str, CompositeRule> {
    alt((
        alt((parse_numeric_rule, parse_string_rule)),
        parse_unary_expr,
        parse_bracket_expr,
    ))(input)
}

impl FromStr for CompositeRule {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_expression(s)
            .map(|(_, x)| x)
            .map_err(|x| x.to_string().into())
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
