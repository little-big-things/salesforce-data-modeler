import duckdb

def generate_mermaid_erd(duckdb_path: str, output_path: str = "output/erd_diagram.md"):
    con = duckdb.connect(duckdb_path)
    
    # Example: union all describe tables. We'll just list them by searching DuckDB for those tables:
    table_list = con.execute("SHOW TABLES").fetchall()
    desc_tables = [t[0] for t in table_list if t[0].startswith("Describe_")]
    
    with open(output_path, "w") as f:
        f.write("erDiagram\n")
        for dt in desc_tables:
            df_desc = con.execute(f"SELECT * FROM {dt}").fetchdf()
            object_name = dt.replace("Describe_", "")
            f.write(f"  {object_name} {{\n")
            for _, row in df_desc.iterrows():
                f.write(f"    {row['field_type']} {row['field_api_name']} \"{row['field_label']}\"\n")
            f.write("  }\n")
    
    con.close()
    print(f"ERD diagram generated at {output_path}")

# Example usage
if __name__ == "__main__":
    generate_mermaid_erd("data/my_data.duckdb")
