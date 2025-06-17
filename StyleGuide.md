# SQL

## General
- All SQL must end with a `;`
- When accessing variables, always use `{0}` with a specific number rather than the implicit `{}`
- Indentations should be 4 spaces each (except when specifically aligning with an operator)

## Line Spacing
- There should be a new line at the end of the file
- There should be no blank lines after a `WITH` statement or after comments
- There should be one blank line between a user-defined function and the rest of the code

## In-line Spacing
- Commas should be followed by a space
- All SQL operators should have a space preceding and following them (`col1 + col2` is correct, `col1+col2` is not)

## Indentation
- The main `SELECT`, `FROM`, ect should not be indented
- The values we are selecting should be indented once
- `AND`/`OR` statements in the `WHERE` should be indented once
- If a statement is wrapped in a grouping parenthesis, it should be indented once extra
- A `JOIN` should be indented once
  - The `ON` of the `JOIN` should be indented once more.
  - If there are multiple statements in the `ON` they should be aligned evenly with the `ON` on different lines
- Statements inside a `WITH` should be tabbed over one extra time

## Casing
- All word operators (`AND`, `OR`, `AS`, `IN`) and types (`DATE`, `NUMERIC`) should be capitalized

## Operators
- Always use `<>` for `Does not equal`. Do not use `!=`
- When comparing case insensitive values, always use `UPPER` to standardize (not `LOWER`)
- `AND` and `OR` operators should be placed at the beginning of a new line, not at the end of the previous one
- Comparison and mathematical operators should be at the end of the line if the comparison or equation takes multiple lines
- When comparing values, the comparison should be on one line `col1 <> col2` unless there are multiple values being compared on either side, in which case all values should be on their own line

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

# MD Files

## Tabbing
Spaces should be used in place of tabs for indentation, 4 spaces to a level

## Bullets
Bulleted lists are denoted using `-`

## Escaping
All underscores not surrounded by backticks must be properly escaped

## Code Blocks
Code blocks should be surrounded by three backticks, not indented.
- Exception: If three backticks are needed inside the code block, it is ok to create one using tabbing instead


# API Documentation

## General
- All rules in the MD Files section should be followed
- Each API endpoint should have its own file titled the same as the endpoint and placed in the folder relating to the `routes` file it is found in.
- API endpoints should be top-level headers (`#`)
- Details about an API endpoint should be second-level headers (`##`)
- Required body values, params, and request values must go before non-required ones.

## GET Endpoints
Sample GET endpoint:

    # GET "/v1/end\_point/"
    Details about endpoint
    
    ## Sample Request
    `/v1/end_point/?params=here&other=params`
    
    ## Request Params
    Any general details (if needed) about the params. Otherwise leave this paragraph out.
    
    - `params`: (required, string) This param does something. Acceptable values:
        - `here`
        - `there`
    - `other`: (string) This param does other things
    
    ## Response (JSON)
    ```
    {
        "response_1": 1234,
        "response_2": "Another one"
    }
    ```
    
    ## Response Attributes
    - `response_1`: (integer) what the response describes
    - `response_2`: (string) what the second response describes. Possible values:
        - `The first one`: a value
        - `Another one`: a second value
    
    ## Errors
    Possible HTTP Status Codes:
    
    - 400:
        - Reason 1 for 400 error
        - Reason 2 for 400 error
    - 404: Single reason for 404 error

## POST Endpoints
Sample POST endpoint:

    # POST "/v1/end\_point/"
    Details about endpoint
    
    ## Body (JSON)
    ```
    {
        "A": "Contents",
        "B": 1234
    }
    ```
    
    ## Body Description
    Any general details (if needed) about the body. Otherwise leave this paragraph out.
    
    - `A`: (required, string) Does something.
    - `B`: (integer) Does other things
    
    ## Response (JSON)
    ```
    {
        "response_1": 1234,
        "response_2": "Another one"
    }
    ```
    
    ## Response Attributes
    - `response_1`: (integer) what the response describes
    - `response_2`: (string) what the second response describes. Possible values:
        - `The first one`: a value
        - `Another one`: a second value
    
    ## Errors
    Possible HTTP Status Codes:
    
    - 400:
        - Reason 1 for 400 error
        - Reason 2 for 400 error
    - 404: Single reason for 404 error