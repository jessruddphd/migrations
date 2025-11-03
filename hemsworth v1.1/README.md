
#Query Migration Tool is an agent-driven workflow. Cascade should run the workflow end-to-end on behalf of the user.

## Setup

1. Create a virtual environment. Use always python3. **Requires Python 3.8 or higher**.
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running Scripts
- Always use `python3` (not `python`) to run scripts in this project, e.g.:
  ```
  python3 scripts/delta_lookup.py schema.table.column
  ```
- The script `scripts/delta_lookup.py` requires input in the form `schema.table` or `schema.table.column` (e.g., `fdg.fact_casino_reward_spin_can.reel_1`). Passing only a column name will result in an error.

## Project Structure

- `source_files/`: Source files directory. The user puts their files here that they want migrated.
- `output_files/`: Output files directory. The agent puts new files here after the conversion.
- `examples/`: Examples directory. The agent puts example files here.
- `scripts/`: Scripts directory. The agent puts scripts here.
- `requirements.txt`: Project dependencies
- `README.md`: Project documentation
- `workspace_rules.md`: Workspace rules for query migration

## Query Migration Workflow

To ensure consistent and correct query migration, all users should follow these steps when running the SQL conversion workflow found in workspace_rules.md.

## SQL Best Practices
See [AGENTS.md](AGENTS.md) for comprehensive SQL optimization guidelines when working with migrated queries.
