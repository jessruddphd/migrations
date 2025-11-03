import sys
import json
import os

def load_mapping(json_path):
    with open(json_path, 'r') as f:
        return json.load(f)

def lookup(mapping, redshift_path):
    parts = redshift_path.split('.')
    if len(parts) not in (2, 3):
        print("Input must be of the form schema.table or schema.table.column", file=sys.stderr)
        sys.exit(1)
    schema, table = parts[0], parts[1]
    column = parts[2] if len(parts) == 3 else None

    # Traverse mapping strictly
    if schema not in mapping:
        print(f"Schema '{schema}' not found in mapping.", file=sys.stderr)
        sys.exit(2)
    if table not in mapping[schema]:
        print(f"Table '{table}' not found in schema '{schema}'.", file=sys.stderr)
        sys.exit(3)
    if column:
        if column not in mapping[schema][table]:
            print(f"Column '{column}' not found in table '{table}' in schema '{schema}'.", file=sys.stderr)
            sys.exit(4)
        col_map = mapping[schema][table][column]
        delta_catalog = col_map.get('delta_catalog', '')
        delta_schema = col_map.get('delta_schema', '')
        delta_table = col_map.get('delta_table', '')
        delta_column = col_map.get('delta_column', '')
        if not (delta_catalog and delta_schema and delta_table and delta_column):
            print(f"Delta mapping incomplete for {schema}.{table}.{column}", file=sys.stderr)
            sys.exit(5)
        print(f"{delta_catalog}.{delta_schema}.{delta_table}.{delta_column}")
    else:
        # Table-level mapping: print all columns
        columns = mapping[schema][table]
        results = set()
        for col, col_map in columns.items():
            delta_catalog = col_map.get('delta_catalog', '')
            delta_schema = col_map.get('delta_schema', '')
            delta_table = col_map.get('delta_table', '')
            if delta_catalog and delta_schema and delta_table:
                results.add(f"{delta_catalog}.{delta_schema}.{delta_table}")
        if results:
            for r in sorted(results):
                print(r)
        else:
            print(f"No Delta mapping found for table {schema}.{table}", file=sys.stderr)
            sys.exit(6)

def main():
    if len(sys.argv) != 2:
        print("Usage: python redshift_to_delta_lookup.py schema.table[.column]", file=sys.stderr)
        sys.exit(1)
    json_path = os.path.join(os.path.dirname(__file__), 'redshift_to_delta.json')
    mapping = load_mapping(json_path)
    lookup(mapping, sys.argv[1])

if __name__ == "__main__":
    main()
