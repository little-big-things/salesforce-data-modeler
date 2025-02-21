import duckdb
import pandas as pd
from simple_salesforce import Salesforce
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the root directory
load_dotenv()

def connect_to_salesforce():
    username = os.getenv('SALESFORCE_USERNAME')
    password = os.getenv('SALESFORCE_PASSWORD')
    security_token = os.getenv('SALESFORCE_SECURITY_TOKEN')
    domain = os.getenv('SALESFORCE_DOMAIN')

    # Create a Salesforce connection
    sf = Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        domain=domain
    )
    return sf

def get_relationships(sf, object_name):
    # Describe the object to get metadata
    obj_metadata = getattr(sf, object_name).describe()
    
    # Extract relationship fields
    relationships = []
    for field in obj_metadata['fields']:
        if field['type'] in ['reference']:
            for ref in field['referenceTo']:
                relationships.append({
                    'field_name': field['name'],
                    'relationship_name': field['relationshipName'],
                    'reference_to': ref,
                    'is_primary_key': field['name'] == 'Id',
                    'is_foreign_key': True
                })
    
    return relationships

def store_metadata_in_duckdb(sf, object_names, db_path='salesforce_metadata.db'):
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    # Create metadata table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS salesforce_metadata (
            object_name VARCHAR,
            field_name VARCHAR,
            field_type VARCHAR,
            relationship_name VARCHAR,
            reference_to VARCHAR,
            is_primary_key BOOLEAN,
            is_foreign_key BOOLEAN
        )
    """)
    
    # Process each object and store metadata
    for object_name in object_names:
        obj_metadata = getattr(sf, object_name).describe()
        for field in obj_metadata['fields']:
            relationships = get_relationships(sf, object_name)
            for rel in relationships:
                conn.execute("""
                    INSERT INTO salesforce_metadata (
                        object_name,
                        field_name,
                        field_type,
                        relationship_name,
                        reference_to,
                        is_primary_key,
                        is_foreign_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    object_name,
                    field['name'],
                    field['type'],
                    rel['relationship_name'],
                    rel['reference_to'],
                    rel['is_primary_key'],
                    rel['is_foreign_key']
                ))
    
    conn.close()
    print(f"Metadata stored in DuckDB at {db_path}")

def main():
    sf = connect_to_salesforce()
    
    # Example list of objects to analyze
    object_names = ['Account', 'Contact', 'Opportunity']  # Add more objects as needed
    
    # Store metadata in DuckDB
    store_metadata_in_duckdb(sf, object_names)

if __name__ == "__main__":
    main()
