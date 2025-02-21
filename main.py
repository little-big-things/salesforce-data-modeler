from pipeline.sf_connection import connect_to_salesforce, get_all_metadata
from pipeline.sf_to_duckdb import store_metadata_in_duckdb, verify_relationships

def main():
    try:
        # Connect to Salesforce
        print("Connecting to Salesforce...")
        sf = connect_to_salesforce()
        
        # Get metadata
        print("Retrieving Salesforce metadata...")
        metadata = get_all_metadata(sf)
        
        # Store in DuckDB
        print("Storing metadata in DuckDB...")
        store_metadata_in_duckdb(sf, metadata)
        
        # Verify relationships were stored
        verify_relationships()
        
        print("Successfully stored Salesforce metadata in DuckDB")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main() 