import duckdb
import json
from datetime import datetime

def store_metadata_in_duckdb(sf, metadata, db_path='salesforce_metadata.db'):
    """
    Store Salesforce metadata and relationships in DuckDB
    """
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    relationship_count = 0
    
    try:
        # Create metadata table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS salesforce_metadata (
                timestamp TIMESTAMP,
                object_name VARCHAR,
                label VARCHAR,
                custom BOOLEAN,
                queryable BOOLEAN,
                fields JSON,
                metadata JSON
            )
        """)
        
        # Create relationships table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS salesforce_relationships (
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
            if not sobject['queryable']:
                continue

            # Get detailed metadata for this object
            print(f"Getting detailed metadata for {sobject['name']}...")
            detailed_metadata = getattr(sf, sobject['name']).describe()
            
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
                sobject['label'],
                sobject['custom'],
                sobject['queryable'],
                json.dumps(detailed_metadata['fields']),
                json.dumps(detailed_metadata)
            ))
            
            # Store relationships
            for field in detailed_metadata['fields']:
                # Store primary key (Id field)
                if field['type'] == 'id':
                    relationship_count += 1
                    print(f"Found primary key: {sobject['name']}.{field['name']}")
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
                        '',  # No relationship name for primary key
                        sobject['name'],  # References itself
                        True,
                        False
                    ))
                
                # Store foreign key relationships
                if field['type'] == 'reference' and field.get('referenceTo'):
                    for ref in field['referenceTo']:
                        relationship_count += 1
                        print(f"Found foreign key relationship: {sobject['name']}.{field['name']} -> {ref}")
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
                            False,
                            True
                        ))
        
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
    
    # Check primary keys
    print("\nSample Primary Keys:")
    result = conn.execute("""
        SELECT source_object, field_name
        FROM salesforce_relationships 
        WHERE is_primary_key = true
        LIMIT 5
    """).fetchall()
    for row in result:
        print(f"{row[0]}.{row[1]} (Primary Key)")
    
    # Check foreign keys
    print("\nSample Foreign Key Relationships:")
    result = conn.execute("""
        SELECT source_object, field_name, reference_to
        FROM salesforce_relationships 
        WHERE is_foreign_key = true
        LIMIT 5
    """).fetchall()
    for row in result:
        print(f"{row[0]}.{row[1]} -> {row[2]}")
    
    conn.close() 