# Redshift to Databricks SQL Syntax Reference

## Date and Time Functions

| Redshift | Databricks | Notes |
|----------|------------|-------|
| `SYSDATE` | `CURRENT_TIMESTAMP()` | Current timestamp |
| `DATEDIFF(datepart, start, end)` | `DATEDIFF(end, start)` | **Parameter order reversed!** |

## Data Type Conversions

| Redshift | Databricks | Notes |
|----------|------------|-------|
| `expr::type` | `CAST(expr AS type)` | PostgreSQL-style casting not supported |


## Data Types

| Redshift | Databricks | Notes |
|----------|------------|-------|
| `VARCHAR(n)` | `STRING` or `VARCHAR(n)` | STRING is preferred |
| `CHAR(n)` | `STRING` | No fixed-length strings |
| `TEXT` | `STRING` | Use STRING |
| `INTEGER` | `INT` | Same concept |
| `DOUBLE PRECISION` | `DOUBLE` | Use DOUBLE |


## Regular Expressions

| Redshift | Databricks | Notes |
|----------|------------|-------|
| `string ~ pattern` | `RLIKE(string, pattern)` | Different syntax |


## Common Gotchas

### 1. **Parameter Order Changes**
- `DATEDIFF(start, end)` in Redshift → `DATEDIFF(end, start)` in Databricks

### 2. **Array Indexing**
- Redshift arrays start at 1 → Databricks arrays start at 0

### 3. **String Concatenation**
- Redshift: `string1 + string2`
- Databricks: `CONCAT(string1, string2)` or `string1 || string2`

### 4. **Case Sensitivity**
- Redshift: Case-insensitive by default
- Databricks: Case-sensitive for identifiers

### 5. **Implicit Type Conversions**
- Redshift: More permissive implicit conversions
- Databricks: Stricter type checking, may need explicit CAST

### 6. **LIMIT vs TOP**
- Redshift: `SELECT TOP 10 * FROM table` or `SELECT * FROM table LIMIT 10`
- Databricks: `SELECT * FROM table LIMIT 10` (no TOP keyword)

## Migration Best Practices

1. **Always use explicit CAST** instead of relying on implicit conversions
2. **Test date arithmetic carefully** - parameter orders may be different
3. **Replace proprietary functions** with standard SQL equivalents

## Functions to Avoid/Replace

| Avoid in Databricks | Use Instead | Reason |
|-------------------|-------------|---------|
| `string1 + string2` | `CONCAT()` or `\|\|` | Different behavior |
