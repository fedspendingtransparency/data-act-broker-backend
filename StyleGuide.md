# Python Comments

## General
* Follow [this guide](https://google.github.io/styleguide/pyguide.html#Comments) in general (with some differences), details below
* Only function comments should be block. Every code-level comment (even multi-line) should be single-line comments denoted with `#`

## Single-line comments
* Space after the `#`

## Single-line block comments
* Space between the text and the quotes: `""" Text here. """`

## Multi-line block comments
* Space after the first set of quotes
* Start text on the same line as the opening quotes
* All subsequent lines have a tab of indentation to align text with higher lines
* Closing quotes are aligned with opening quotes and on their own line

```
""" This is the first line of this comment
    This is the second line
"""
```

## Function comments
* Can be multi- or single-line depending on need
* When a function has arguments, returns, or raises an exception those should be detailed in the comment
* Details within the above sections should be tabbed over another time
* If one or more of these sections is not applicable, do not include them
* Extra line between each section of the comment

```
""" First line of comment describing things

    Args:
        Arg1: description
        Arg2: description
    
    Returns:
        Description of what is returned here
        
    Raises:
        Exception: Something broke, explain
"""
```

# SQL

## General
* All SQL must end with a `;`
* When accessing variables, always use `{0}` with a specific number rather than the implicit `{}`
* Indentations should be 4 spaces each (except when specifically aligning with an operator)

## Line Spacing
* There should be a new line at the end of the file
* There should be no blank lines after a `WITH` statement or after comments
* There should be one blank line between a user-defined function and the rest of the code

## In-line Spacing
* Commas should be followed by a space
* All SQL operators should have a space preceding and following them (`col1 + col2` is correct, `col1+col2` is not)

## Indentation
* The main `SELECT`, `FROM`, ect should not be indented
* The values we are selecting should be indented once
* `AND`/`OR` statements in the `WHERE` should be indented once
* If a statement is wrapped in a grouping parenthesis, it should be indented once extra
* A `JOIN` should be indented once
  * The `ON` of the `JOIN` should be indented once more.
  * If there are multiple statements in the `ON` they should be aligned evenly with the `ON` on different lines
* Statements inside a `WITH` should be tabbed over one extra time

## Casing
* All word operators (`AND`, `OR`, `AS`, `IN`) and types (`DATE`, `NUMERIC`) should be capitalized

## Operators
* Always use `<>` for `Does not equal`. Do not use `!=`
* When comparing case insensitive values, always use `UPPER` to standardize (not `LOWER`)
* `AND` and `OR` operators should be placed at the beginning of a new line, not at the end of the previous one
* Comparison and mathematical operators should be at the end of the line if the comparison or equation takes multiple lines
* When comparing values, the comparison should be on one line `col1 <> col2` unless there are multiple values being compared on either side, in which case all values should be on their own line

```
col1 <>
col2 +
col3
```

The exception to the above rule is when 2 values are intricately linked

```
col1 <>
col2 +
col3_A + col3_B +
col4
```

## Parenthesis
Parenthesis wrapping logical groups should close on a new line in the same plane as where the statement started. The first statement of these groups should be on the same line as the opening parenthesis, which should be on the same line as the operator that precedes it.

```
WHERE (col1 IS NULL
	OR col2 IS NULL
)
```
### Exceptions:

In a `WITH` statement, the opening parenthesis should be on the line after the `AS` and the closing parenthesis should not be on a new line.

```
WITH tmp_table AS
	(SELECT a,
		b
	FROM table1
	WHERE a > b)
```
In an `EXISTS` statement, the opening parenthesis should be on the same line as the `EXISTS` but the `SELECT` should start on the next line.

In a `CASE` statement, the closing parenthesis should be on the same line as the `END`.

## SELECT
The first column being selected should be on the same line as the `SELECT` except in the main `SELECT` of the entire SQL statement, where it should start on the next line.

Main statement:

```
SELECT
	col1,
	col2
```

All other statements:

```
SELECT col1,
	col2
```

## CASE
Case operations should be formatted as follows:

```
CASE WHEN statement
	THEN statement
	ELSE statement
END
```
Comparators for `CASE` statements should be on the same line as the `END` statement

```
WHERE (CASE WHEN statement
			THEN statement
			ELSE statement
		END) > col2
```

## EXISTS
The `SELECT` in an `EXISTS` statement should always select `1`