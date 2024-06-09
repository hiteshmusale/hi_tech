# bookings_etl/assets/supabase/supabase_public_bookings.py
from dagster import asset, Output, AssetIn, MetadataValue, AssetKey, Field, Int
from bookings_etl.utils import *
from bookings_etl.resources.supabase import *
import pandas as pd
import numpy as np

def create_supabase_public_booking_values_asset(tenant_id, tenant_name):
    """Factory function to create a booking values asset for a specific tenant."""
    
    bookings_asset_name = f"supabase_public_bookings_{tenant_name}"
    
    @asset(
        name=f"supabase_public_booking_values_{tenant_name}",
        group_name=f"bookings_etl_{tenant_name}",
        metadata={
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "description": f"Asset representing the 'booking_values' table in Supabase public schema for tenant {tenant_name}."
        },
        required_resource_keys={"supabase"},
        ins={
            "supabase_public_bookings": AssetIn(AssetKey(bookings_asset_name))
        }
    
    )
    def supabase_public_booking_values(context, supabase_public_bookings):
        # Transform the data
        upsert_dict = None
        context.log.info(f"Processed {len(upsert_dict)} booking records for tenant {tenant_name} ({tenant_id}).")
        
        # Find unmatched rentals
        unmatched_rentals = [entry for entry in upsert_dict if entry['rental_id'] is None]
        num_unmatched = len(unmatched_rentals)
        
        # Return the upserted data
        metadata = {
            "num_bookings": len(upsert_dict),
            "preview": upsert_dict[:5],
            "nunmatched_rental_count": num_unmatched,
            "unmatched_rental_rows": unmatched_rentals[:10]
        }
        
        # Upsert to supabase
        """
        try:
            # Upsert to supabase
            supabase = context.resources.supabase
            if supabase_upsert(supabase, "bookings", upsert_dict):
                context.log.info(f"Upserted {len(upsert_dict)} rental records for tenant {tenant_name} ({tenant_id}).")
            else:
                raise Exception(f"Failed to upsert rental records for tenant {tenant_name} ({tenant_id}).")
        except Exception as e:
            error_message = f"Error during upsert for tenant {tenant_name} ({tenant_id}): {str(e)}"
            context.log.error(error_message)
            raise
        return Output(upsert_dict, metadata=metadata)
        """
    
    return supabase_public_booking_values
"""
def transform_supabase_public_bookings(bookings_df, rentals_df, tenant_id):
    # Rename columns lower case and replace spaces with underscores
    bookings_df.columns = bookings_df.columns.str.lower().str.replace(' ', '_')
    
    # Clean booking ids
    bookings_df["id"] = bookings_df["booking_id"].apply(clean_uuid)
    
    # Add a new column "tenant_id" with the tenant ID
    bookings_df["tenant_id"] = tenant_id
    
    # Add rental_id column
    # Ensure the columns 'rental' and 'name' are properly trimmed and lowercased for matching
    bookings_df['rental_name_match'] = bookings_df['rental'].str.strip().str.lower()
    rentals_df['rental_name'] = rentals_df['Name'].str.strip().str.lower()
    
    # Prepare the rentals DataFrame for merging
    rentals_df["rental_id"] = rentals_df["Pkey"].apply(clean_uuid)
    rentals_df = rentals_df[["rental_id", "rental_name"]]

    # Merge the DataFrames to add the rental_id to the bookings DataFrame
    bookings_df = pd.merge(
        bookings_df,
        rentals_df,
        left_on='rental_name_match',
        right_on='rental_name',
        how='left'
    )
    
    # Replace NaN values with None
    bookings_df = bookings_df.replace({np.nan: None})
    
    # Keep only required columns
    required_columns = [
    "id",
    "tenant_id",
    "name",
    "email",
    "guest_secondary_emails",
    "booking_status",
    "rental",
    "arrive",
    "depart",
    "nights",
    "received",
    "booking_id",
    "inquiry_id",
    "source",
    "adults",
    "children",
    "total_cost",
    "rental_id"
]
    bookings_df = bookings_df[required_columns]
    
    # Convert datetime columns to ISO 8601 strings
    datetime_columns = ["arrive", "depart", "received"]
    for col in datetime_columns:
        if col in bookings_df.columns:
            bookings_df[col] = pd.to_datetime(bookings_df[col]).dt.strftime('%Y-%m-%dT%H:%M:%S')

    return bookings_df.to_dict(orient='records')
"""