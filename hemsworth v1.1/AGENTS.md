# SQL Best Practices & Optimization Agent

This guide outlines best practices for writing efficient, readable, and optimized SQL queries.

## 1. Query Structure & Readability

Readability is key to maintainability.

- **Keywords**: Use UPPERCASE for SQL keywords (SELECT, FROM, WHERE, GROUP BY, ORDER BY, JOIN).
- **Identifiers**: Use lowercase or snake_case for your table and column names (user_profiles, first_name).
- **Indentation**: Use indentation to clearly define logical blocks, especially with subqueries and complex joins.
- **Comments**: Use `--` for single-line comments and `/* ... */` for multi-line comments to explain complex logic.

**Example:**
```sql
/*
 * Selects all active users from the US
 * and counts their total orders.
 */
SELECT
    u.user_id,
    u.first_name,
    COUNT(o.order_id) AS total_orders
FROM
    users AS u
JOIN
    orders AS o
    ON u.user_id = o.user_id
WHERE
    u.status = 'active'
    AND u.country = 'US'
GROUP BY
    u.user_id,
    u.first_name
ORDER BY
    total_orders DESC;
```

## 2. Query Optimization (The WHERE Clause)

The WHERE clause is the most critical part of query optimization. Filter as much data as possible, as early as possible.

- **SELECT ***: Avoid using `SELECT *`. Explicitly name the columns you need. This reduces I/O, network traffic, and memory usage.

- **SARGable Queries**: Ensure your WHERE conditions are "SARGable" (Search ARGument-able), meaning they can use an index.
  - **DO**: `WHERE user_id = 123`
  - **DO**: `WHERE created_at >= '2025-01-01'`
  - **DO**: `WHERE name LIKE 'John%'` (Index can be used)
  - **DON'T**: `WHERE YEAR(created_at) = 2025` (Applies a function to the column)
  - **DON'T**: `WHERE name = 'John'` (If name is NVARCHAR and you provide VARCHAR)
  - **DON'T**: `WHERE name LIKE '%John'` (Index cannot be used)

- **Avoid Functions on Columns**: Applying functions to columns in the WHERE clause (e.g., UPPER(), DATE(), SUBSTRING()) prevents the database from using indexes on that column. Move the logic to the other side of the operator if possible.
  - **Bad**: `WHERE DATE(order_date) = '2025-10-30'`
  - **Good**: `WHERE order_date >= '2025-10-30' AND order_date < '2025-10-31'`

- **OR vs. UNION ALL**: An OR on different columns can be slow. If the conditions are mutually exclusive and indexed, UNION ALL can sometimes be faster.
  - **Slow**: `WHERE indexed_col_A = 'value1' OR indexed_col_B = 'value2'`
  - **Faster (Maybe)**:
    ```sql
    SELECT ... WHERE indexed_col_A = 'value1'
    UNION ALL
    SELECT ... WHERE indexed_col_B = 'value2' AND indexed_col_A <> 'value1'
    ```

## 3. JOIN Optimization

- **INNER JOIN vs. LEFT JOIN**: Use INNER JOIN as the default. Only use LEFT JOIN when you specifically need to include rows from the "left" table that have no match in the "right" table. INNER JOIN is almost always more performant as it reduces the result set early.

- **Join on Indexed Columns**: Always join tables on columns that are indexed (ideally, primary keys (PK) and foreign keys (FK)).

- **Filter Early**: When using LEFT JOIN, apply WHERE conditions for the right table in the ON clause, and conditions for the left table in the WHERE clause.

**Example:**
```sql
SELECT
    u.name,
    o.order_details
FROM
    users AS u
LEFT JOIN
    orders AS o
    -- Filter the RIGHT table (orders) here
    ON u.user_id = o.user_id
    AND o.status = 'shipped'
WHERE
    -- Filter the LEFT table (users) here
    u.is_active = 1;
```

## 4. Indexing Strategy

Indexes are the single most effective way to speed up SELECT queries, but they slow down INSERT, UPDATE, and DELETE.

**What to Index:**
- Primary Keys (automatic)
- Foreign Keys (almost always)
- Columns frequently used in WHERE clauses
- Columns frequently used in ORDER BY clauses

- **Composite Indexes**: If you frequently filter on multiple columns, create a composite (multi-column) index. The order of columns in the index matters.
  - If you query `WHERE last_name = 'Smith' AND first_name = 'John'`, your index should be on `(last_name, first_name)`.

- **EXPLAIN Plan**: Use EXPLAIN (or EXPLAIN ANALYZE) before your SELECT statement.
  - Look for "Full Table Scan". This is bad and means no index is being used.
  - Look for "Index Scan" or "Index Seek". This is good.

- **Don't Over-Index**: Too many indexes will slow down write operations. Find a balance.

## 5. GROUP BY and COUNT

- **COUNT(*) vs. COUNT(1) vs. COUNT(column)**:
  - `COUNT(*)`: Generally the fastest and most standard way to count all rows.
  - `COUNT(1)`: Identical in performance to COUNT(*) in modern databases.
  - `COUNT(column_name)`: Counts non-NULL values in that specific column. Use this only when you need to exclude NULLs.

- **EXISTS vs. IN vs. JOIN**:
  - **EXISTS**: Use when you just need to check if a matching row exists. It's often the fastest as it stops processing as soon as it finds one match.
    ```sql
    -- Find users who have placed at least one order
    SELECT u.name FROM users u
    WHERE EXISTS (
        SELECT 1 FROM orders o WHERE o.user_id = u.user_id
    );
    ```
  - **IN**: Good for short, static lists. Can be slow with large subqueries.
  - **JOIN**: Use when you also need to retrieve data from the joined table.

## 6. Schema Design Best Practices

- **Normalization**: Start with a normalized (3NF) design to reduce data redundancy. Only denormalize intentionally for specific performance reasons (e.g., in a data warehouse).

- **Data Types**: Use the smallest, most appropriate data type for each column.
  - Don't use VARCHAR(2000) for a first_name column. Use VARCHAR(100).
  - Use INT instead of BIGINT if you don't need the range.
  - Use DATE or TIMESTAMP instead of VARCHAR to store dates.

- **NULLs**: Avoid nullable columns (ALLOW NULL) unless you have a specific reason. NOT NULL constraints are more efficient and prevent data integrity issues.

## 7. Common Pitfalls

- **N+1 Queries**: This is a common application-level problem. Don't run a query inside a loop.
  - **Bad (in code)**:
    ```sql
    users = query("SELECT user_id FROM users WHERE country = 'US'")
    for user in users:
        orders = query("SELECT * FROM orders WHERE user_id = ?", user.user_id)
    ```
  - **Good (in code)**:
    ```sql
    query("SELECT ... FROM users u JOIN orders o ON u.user_id = o.user_id WHERE u.country = 'US'")
    // OR
    query("SELECT * FROM orders WHERE user_id IN (SELECT user_id FROM users WHERE country = 'US')")
    ```

- **Implicit Type Conversion**: If you compare a VARCHAR column to a number (`WHERE varchar_col = 123`), the database must convert one side, which prevents index use. Always match data types.

## 8. Databricks-Specific Optimizations

- **Delta Lake Features**: Leverage Delta Lake's OPTIMIZE and Z-ORDER commands for better performance.
  ```sql
  OPTIMIZE table_name ZORDER BY (frequently_filtered_column);
  ```

- **Partitioning**: Use partitioning on frequently filtered columns (especially date columns).
  ```sql
  CREATE TABLE partitioned_table (...)
  PARTITIONED BY (date_column);
  ```

- **Caching**: Use caching for frequently accessed tables.
  ```sql
  CACHE TABLE frequently_used_table;
  ```

- **Broadcast Joins**: For small dimension tables, use broadcast hints.
  ```sql
  SELECT /*+ BROADCAST(small_table) */ *
  FROM large_table l
  JOIN small_table s ON l.id = s.id;
  ```

## 9. Migration-Specific Best Practices

- **Incremental Processing**: When migrating large datasets, process data incrementally using date ranges.
- **Data Validation**: Always validate row counts and key metrics after migration.
- **Rollback Strategy**: Maintain ability to rollback to previous version if issues arise.
- **Performance Testing**: Test query performance on representative data volumes before production deployment.
