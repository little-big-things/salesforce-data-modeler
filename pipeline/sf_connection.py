import json
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

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

def get_all_metadata(sf):
    """
    Retrieve all metadata including detailed object descriptions
    """
    # Get global describe first
    print("Getting global describe...")
    metadata = sf.describe()
    
    # Get detailed metadata for each queryable object
    detailed_metadata = metadata.copy()
    detailed_metadata['sobjects'] = []
    
    for sobject in metadata['sobjects']:
        if sobject['queryable']:
            print(f"Getting detailed metadata for {sobject['name']}...")
            try:
                # Get detailed description for this object
                detailed_obj = getattr(sf, sobject['name']).describe()
                detailed_metadata['sobjects'].append(detailed_obj)
            except Exception as e:
                print(f"Warning: Could not get detailed metadata for {sobject['name']}: {str(e)}")
                continue
    
    print(f"Retrieved detailed metadata for {len(detailed_metadata['sobjects'])} objects")
    return detailed_metadata

def get_all_object_names(sf):
    """
    Get a list of all object names from Salesforce, including custom objects
    """
    # Get global describe
    metadata = sf.describe()
    
    # Extract all object names that are queryable
    object_names = [
        obj['name'] 
        for obj in metadata['sobjects'] 
        if obj['queryable']  # Only include queryable objects
    ]
    
    print(f"Found {len(object_names)} queryable objects in Salesforce")
    return object_names

# Example usage
if __name__ == "__main__":
    sf = connect_to_salesforce()
    metadata = get_all_metadata(sf)
    print(metadata)
