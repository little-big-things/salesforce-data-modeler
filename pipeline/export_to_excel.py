import duckdb
import pandas as pd
import json
from datetime import datetime

def export_metadata_to_excel(db_path='salesforce_metadata.db', output_file='salesforce_data_model.xlsx'):
    """
    Export Salesforce metadata and relationships to Excel format suitable for data modeling
    """
    conn = duckdb.connect(db_path)
    
    try:
        # Get the latest metadata for each object
        results = conn.execute("""
            WITH LatestMetadata AS (
                SELECT 
                    object_name,
                    fields,
                    custom,
                    ROW_NUMBER() OVER (PARTITION BY object_name ORDER BY timestamp DESC) as rn
                FROM salesforce_metadata
            )
            SELECT 
                object_name,
                fields,
                custom
            FROM LatestMetadata
            WHERE rn = 1
            ORDER BY object_name
        """).fetchall()
        
        objects_data = []
        for result in results:
            object_name, fields_json, is_custom = result
            fields = json.loads(fields_json)
            
            for field in fields:
                # Extract essential field metadata
                field_data = {
                    'Object': object_name,
                    'Custom Object': is_custom,
                    'Field Name': field['name'],
                    'Label': field.get('label', ''),
                    'Type': field['type'],
                    'Length': field.get('length', ''),
                    'Precision': field.get('precision', ''),
                    'Scale': field.get('scale', ''),
                    'Required': not field.get('nillable', True),
                    'Unique': field.get('unique', False),
                    'Default Value': field.get('defaultValue', ''),
                    'Help Text': field.get('inlineHelpText', ''),
                    'Picklist Values': ', '.join([p['value'] for p in field.get('picklistValues', [])]),
                    'Formula': field.get('calculatedFormula', ''),
                    'Description': field.get('inlineHelpText', ''),
                    'Is External ID': field.get('externalId', False),
                    'Is Auto Number': field.get('autoNumber', False),
                    'Is Formula': field.get('calculatedFormula', False),
                    'Is Unique': field.get('unique', False),
                    'Is Encrypted': field.get('encrypted', False),
                    'Is Read Only': field.get('updateable', True) is False,
                    'Is Filterable': field.get('filterable', False),
                    'Is Groupable': field.get('groupable', False),
                    'Is Sortable': field.get('sortable', False),
                }
                objects_data.append(field_data)
        
        # Convert to DataFrame for fields
        df = pd.DataFrame(objects_data)
        
        # Get relationships from the relationships table
        relationships = conn.execute("""
            SELECT 
                source_object,
                field_name,
                relationship_name,
                reference_to,
                is_primary_key
            FROM salesforce_relationships
        """).fetchall()
        
        rel_data = []
        for rel in relationships:
            rel_data.append({
                'Source Object': rel[0],
                'Field Name': rel[1],
                'Relationship Name': rel[2],
                'Referenced Object': rel[3],
                'Is Primary Key': rel[4]
            })
        
        # Convert to DataFrame for relationships
        rel_df = pd.DataFrame(rel_data)
        
        # Export objects list
        objects_df = conn.execute("""
            SELECT DISTINCT object_name, custom
            FROM salesforce_metadata
        """).fetchdf()
        objects_df['type'] = objects_df['custom'].apply(lambda x: 'Custom' if x else 'Standard')
        objects_df.drop(columns=['custom'], inplace=True)
        
        # Write to Excel
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # Write main data
            df.to_excel(writer, sheet_name='Field Definitions', index=False)
            rel_df.to_excel(writer, sheet_name='Relationships', index=False)
            objects_df.to_excel(writer, sheet_name='Objects', index=False)
            
            # Get workbook and worksheets
            workbook = writer.book
            worksheet = writer.sheets['Field Definitions']
            worksheet_rel = writer.sheets['Relationships']
            worksheet_objects = writer.sheets['Objects']
            
            # Add formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#D8E4BC',
                'border': 1
            })
            
            # Format headers and adjust column widths for all sheets
            for sheet, data in [('Field Definitions', df), ('Relationships', rel_df), ('Objects', objects_df)]:
                worksheet = writer.sheets[sheet]
                for col_num, value in enumerate(data.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                    # Adjust column width
                    max_length = max(
                        data[value].astype(str).apply(len).max(),
                        len(str(value))
                    )
                    worksheet.set_column(col_num, col_num, max_length + 2)
        
        print(f"Metadata exported to {output_file}")
        return True
        
    finally:
        conn.close() 