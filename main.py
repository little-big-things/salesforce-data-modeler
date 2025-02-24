import typer
from typing import Optional
from pathlib import Path
from pipeline.sf_connection import connect_to_salesforce, get_all_metadata
from pipeline.sf_to_duckdb import store_metadata_in_duckdb, verify_relationships
from pipeline.export_to_excel import export_metadata_to_excel

app = typer.Typer(help="Salesforce Metadata and Documentation Generator")

@app.command()
def refresh(
    db_path: Path = typer.Option('salesforce_metadata.db', '--db', '-d', help="Path to DuckDB database"),
    verify: bool = typer.Option(True, '--verify/--no-verify', help="Verify relationships after refresh")
):
    """Refresh metadata from Salesforce"""
    try:
        typer.echo("Connecting to Salesforce...")
        sf = connect_to_salesforce()
        
        typer.echo("Retrieving Salesforce metadata...")
        metadata = get_all_metadata(sf)
        
        typer.echo("Storing metadata in DuckDB...")
        store_metadata_in_duckdb(sf, metadata, str(db_path))
        
        if verify:
            verify_relationships()
        
        typer.echo("Successfully refreshed metadata")
    except Exception as e:
        typer.echo(f"Error: {str(e)}", err=True)
        raise typer.Exit(1)

@app.command()
def excel(
    db_path: Path = typer.Option('salesforce_metadata.db', '--db', '-d', help="Path to DuckDB database"),
    output: Path = typer.Option('salesforce_data_model.xlsx', '--output', '-o', help="Output Excel file path")
):
    """Export metadata to Excel"""
    try:
        typer.echo("Exporting metadata to Excel...")
        export_metadata_to_excel(str(db_path), str(output))
        typer.echo(f"Successfully exported to {output}")
    except Exception as e:
        typer.echo(f"Error: {str(e)}", err=True)
        raise typer.Exit(1)

@app.command()
def all(
    db_path: Path = typer.Option('salesforce_metadata.db', '--db', '-d', help="Path to DuckDB database"),
    excel_output: Path = typer.Option('salesforce_data_model.xlsx', '--excel', help="Output Excel file path"),
    verify: bool = typer.Option(True, '--verify/--no-verify', help="Verify relationships after refresh")
):
    """Run all operations: refresh metadata and export to Excel"""
    try:
        # Refresh
        typer.echo("Connecting to Salesforce...")
        sf = connect_to_salesforce()
        
        typer.echo("Retrieving Salesforce metadata...")
        metadata = get_all_metadata(sf)
        
        typer.echo("Storing metadata in DuckDB...")
        store_metadata_in_duckdb(sf, metadata, str(db_path))
        
        if verify:
            verify_relationships()
        
        # Export to Excel
        typer.echo("Exporting metadata to Excel...")
        export_metadata_to_excel(str(db_path), str(excel_output))
        
        typer.echo("Successfully completed all operations")
    except Exception as e:
        typer.echo(f"Error: {str(e)}", err=True)
        raise typer.Exit(1)

if __name__ == "__main__":
    app() 