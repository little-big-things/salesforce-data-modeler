import duckdb
import json
from datetime import datetime

def get_relationships(sf, object_name):
    """
    Extract relationship fields from a Salesforce object
    """
    obj_metadata = getattr(sf, object_name).describe()
    
    relationships = []
    for field in obj_metadata['fields']:
        if field['type'] == 'reference':
            for ref in field['referenceTo']:
                relationships.append({
                    'field_name': field['name'],
                    'relationship_name': field['relationshipName'],
                    'reference_to': ref,
                    'is_primary_key': field['name'] == 'Id',
                    'is_foreign_key': True
                })
    
    return relationships

def store_metadata_in_duckdb(sf, metadata, db_path='salesforce_metadata.db'):
    """
    Store Salesforce metadata and relationships in DuckDB
    """
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    relationship_count = 0
    
    try:
        # Drop existing tables to ensure clean schema
        conn.execute("DROP TABLE IF EXISTS salesforce_metadata")
        conn.execute("DROP TABLE IF EXISTS salesforce_relationships")
        
        # Create metadata table with correct schema
        conn.execute("""
            CREATE TABLE salesforce_metadata (
                timestamp TIMESTAMP,
                object_name VARCHAR,
                label VARCHAR,
                custom BOOLEAN,
                queryable BOOLEAN,
                fields JSON,
                metadata JSON
            )
        """)
        
        # Create relationships table
        conn.execute("""
            CREATE TABLE salesforce_relationships (
                timestamp TIMESTAMP,
                source_object VARCHAR,
                field_name VARCHAR,
                relationship_name VARCHAR,
                reference_to VARCHAR,
                is_primary_key BOOLEAN,
                is_foreign_key BOOLEAN
            )
        """)
        
        # Process each sObject from the metadata
        for sobject in metadata['sobjects']:
            try:
                if not sobject['queryable']:
                    continue

                # Store main metadata
                conn.execute("""
                    INSERT INTO salesforce_metadata (
                        timestamp,
                        object_name,
                        label,
                        custom,
                        queryable,
                        fields,
                        metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now(),
                    sobject['name'],
                    sobject.get('label', ''),  # Use get() with default value
                    sobject.get('custom', False),
                    sobject.get('queryable', False),
                    json.dumps(sobject.get('fields', [])),
                    json.dumps(sobject)
                ))
                
                # Store relationships
                for field in sobject.get('fields', []):
                    if field['type'] == 'reference' and field.get('referenceTo'):
                        for ref in field['referenceTo']:
                            relationship_count += 1
                            print(f"Found relationship: {sobject['name']}.{field['name']} -> {ref}")
                            conn.execute("""
                                INSERT INTO salesforce_relationships (
                                    timestamp,
                                    source_object,
                                    field_name,
                                    relationship_name,
                                    reference_to,
                                    is_primary_key,
                                    is_foreign_key
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                datetime.now(),
                                sobject['name'],
                                field['name'],
                                field.get('relationshipName', ''),
                                ref,
                                field['name'] == 'Id',
                                True
                            ))
                
            except Exception as e:
                print(f"Warning: Could not process object {sobject['name']}: {str(e)}")
                continue
        
        conn.commit()
        print(f"Total relationships stored: {relationship_count}")
        return True
        
    except Exception as e:
        print(f"Error storing metadata: {str(e)}")
        raise
    finally:
        conn.close()

def verify_relationships():
    conn = duckdb.connect('salesforce_metadata.db')
    
    try:
        # Check primary keys
        print("\nSample Primary Keys:")
        result = conn.execute("""
            SELECT source_object, field_name
            FROM salesforce_relationships 
            WHERE is_primary_key = true
        """).fetchall()
        for row in result:
            print(f"{row[0]}.{row[1]} (Primary Key)")
        
        # Check foreign keys
        print("\nSample Foreign Key Relationships:")
        result = conn.execute("""
            SELECT source_object, field_name, reference_to
            FROM salesforce_relationships 
            WHERE is_foreign_key = true
        """).fetchall()
        for row in result:
            print(f"{row[0]}.{row[1]} -> {row[2]}")
    
    finally:
        conn.close() 