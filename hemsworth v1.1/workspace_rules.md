---
trigger: always_on
---

## 1. The agent should understand that the purpose of this workflow is to help analysts migrate their SQL queries and notebooks from Redshift to Delta Lake (Databricks). The value-add is in re-writing the user's SQL queries to leverage Delta Lake tables and Databricks SQL best practices while leaving alone the rest of the user's code.

## 2. The agent should prompt the user to provide a SQL query or notebook file that they want to migrate.

## 3. How to look up mappings for Redshift schemas, tables, and columns, onto Delta Lake (catalogs), schemas, tables, and columns. The agent should always call the script scripts/delta_lookup.py using the python3 interpreter (i.e., python3 scripts/delta_lookup.py ...). The input must be fully qualified in the form schema.table (for tables) or schema.table.column (for columns). Do not pass only the column name; this will result in an error. The agent should not try to read the mapping file on their own.

## 4. How to use the mappings from the Python script: the agent does not need to put the full reference into the output query for each mapping. For example, it is okay to put "dep.fanduel_user_id" instead of "foundation_views.financial.deposits.fanduel_user_id" into the SELECT statment, if the table "dep" is defined in the FROM statement.

## 5. SQL dialect: The agent must apply the best practices for Databricks SQL found in: https://docs.databricks.com/aws/en/sql/language-manual/. 

## 6. Usable outputs: The agent should make the output file look like the input file as much as possible. Use the examples provided in the examples directory to understand how to write good output files. We've provided an example of a sql. file before and after conversion. We've also provided an example of a .ipynb file before and after conversion. 

## 7. SQL in notebooks: When the source file is a notebook, the agent should focus on the SQL queries found throughout the notebook, leaving the rest of the code in place.

## 8. Missing mappings: for missing mappings, the agent should leave a placeholder in the output query with a comment for the user.