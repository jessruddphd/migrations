---
description: With this workflow, the agent shoudl help the user migrate their SQL queries and notebooks from Redshift to Delta Lake (Databricks).
auto_execution_mode: 1
---

# Query Migration Workflow

1. The agent should read workspace_rules.md to learn the purpose of the workflow and the rules for migrating source files.

2. The agent should prompt the user to provide you a SQL query or notebook file that the user wants to migrate.

3. The agent can begin to analyze the user's source file. Analyze the SQL queries in the source file for all schema, table, and column references in all parts of the query, including the SELECT statement, the FROM statement, the JOINS, WHERE conditions, and so on. 

4. Ask the user if they would like to see a summary of your analysis.

//turbo-all
5. For each table or column reference, the agent must then call scripts/delta_lookup.py to get the column and table mappings to rewrite the query.  Follow the rules in workspace_rules.md for calling the script. The agent may not use generalizations or pattern-matching for the mappings.

//turbo-all
6. After retrieving all mappings, the agent should create a mappings log file in the output_files directory (e.g., "mappings_log_[timestamp].txt") that documents all table and column mappings found. This log serves as a reference to offload mapping details from the context window during the remainder of the workflow. The log should include: original Redshift references, Delta Lake mappings, and any missing mappings with placeholders.

7. After the agent has analyzed the user's source file and retrieved the mappings, ask the user if they would like to see a summary of your findings.

8. The agent can begin to rewrite the queries in the source file. Follow the rules in workspace_rules.md. Reference the mappings log file created in step 6 as needed.

9. Apply SQL Best Practices
Before finalizing migrated queries, review and apply guidelines from AGENTS.md:
- Follow query structure standards
- Optimize WHERE clauses for performance
- Use appropriate JOIN strategies
- Apply Databricks-specific optimizations

//turbo-all
10. After agent has re-written the source file, the agent should store the re-written query or notebook in the directory output_files for the user to review. The new file should match the original file type of the source file. If it's a notebook, the new notebook's metadata should match the original notebook's metadata so that it will run in Databricks. Do not try to edit or overwrite existing files; use a versioning suffix like "_v1."

11. Tell the user where they can find their new file and the mappings log file.