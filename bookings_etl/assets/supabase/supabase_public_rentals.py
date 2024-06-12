# bookings_etl/assets/supabase/supabase_public_rentals.py
from dagster import asset, Output, AssetIn, MetadataValue, AssetKey, Field, Int
from bookings_etl.utils import *
from bookings_etl.resources.supabase import *
import pandas as pd
import numpy as np

def create_supabase_public_rentals_asset(tenant_id, tenant_name):
    """Factory function to create a rentals asset for a specific tenant."""
    
    tokeet_datafeeds_rentals_name = f"tokeet_datafeeds_rentals_{tenant_name}"
    
    @asset(
        name=f"supabase_public_rentals_{tenant_name}",
        group_name=f"bookings_etl_{tenant_name}",
        metadata={
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "description": f"Asset representing the 'rentals' table in Supabase public schema for tenant {tenant_name}."
        },
        required_resource_keys={"supabase"},
        ins={"tokeet_datafeeds_rentals": AssetIn(AssetKey(tokeet_datafeeds_rentals_name))}
    
    )
    def supabase_public_rentals(context, tokeet_datafeeds_rentals):
        # Transform the data
        upsert_dict = transform_supabase_public_rentals(tokeet_datafeeds_rentals, tenant_id)
        context.log.info(f"Processed {len(upsert_dict)} rental records for tenant {tenant_name} ({tenant_id}).")
        
        # Return the upserted data
        metadata = {
            "num_rentals": len(upsert_dict),
            "rental_ids": [rental["id"] for rental in upsert_dict],
            "preview": upsert_dict[:5],
        }
        # Upsert to supabase
        try:
            # Upsert to supabase
            supabase = context.resources.supabase
            if supabase_upsert(supabase, "rentals", upsert_dict):
                context.log.info(f"Upserted {len(upsert_dict)} rental records for tenant {tenant_name} ({tenant_id}).")
            else:
                raise Exception(f"Failed to upsert rental records for tenant {tenant_name} ({tenant_id}).")
        except Exception as e:
            error_message = f"Error during upsert for tenant {tenant_name} ({tenant_id}): {str(e)}"
            context.log.error(error_message)
            raise
            
        return Output(upsert_dict, metadata=metadata)
    
    return supabase_public_rentals

def transform_supabase_public_rentals(tokeet_datafeeds_rentals, tenant_id):
    transformed_data = tokeet_datafeeds_rentals.copy()

    # Rename columns lower case and replace spaces with underscores
    transformed_data.columns = transformed_data.columns.str.lower().str.replace(' ', '_')
    
    # Add a new column "tenant_id" with the tenant ID
    transformed_data["tenant_id"] = tenant_id
    
    # Apply clean_uuid to the "pkey" column and rename it to "id"
    transformed_data["pms_id"] = transformed_data["pkey"]
    transformed_data["id"] = transformed_data["pkey"].apply(clean_uuid)
    transformed_data.drop(columns=["pkey"], inplace=True)
    
    # Rename columns to match the Supabase schema
    transformed_data.rename(columns={
    "address": "address_line1",
    "city": "address_city",
    "state": "address_state",
    "country": "address_country",
    "zip": "address_zip"
}, inplace=True)

    # Keep only required columns
    required_columns = [
    "id",
    "tenant_id",
    "name",
    "address_line1",
    "address_city",
    "address_state",
    "address_country",
    "address_zip",
    "bedrooms",
    "bathrooms",
    "sleep_max",
    "type",
    "gps",
    "pms_id"
]
    transformed_data = transformed_data[required_columns]
    
    # Replace NaN values with None and convert to dict
    transformed_data = transformed_data.replace({np.nan: None})
    upsert_dict = transformed_data.to_dict(orient='records')

    return upsert_dict
