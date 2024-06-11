# bookings_etl/assets/supabase/supabase_public_bookings.py
from dagster import asset, Output, AssetIn, MetadataValue, AssetKey, Field, Int
from bookings_etl.utils import *
from bookings_etl.resources.supabase import *
import pandas as pd
import numpy as np

def create_supabase_public_bookings_asset(tenant_id, tenant_name):
    """Factory function to create a bookings asset for a specific tenant."""
    
    tokeet_datafeeds_bookings_name = f"tokeet_datafeeds_bookings_{tenant_name}"
    tokeet_datafeeds_rentals_name = f"tokeet_datafeeds_rentals_{tenant_name}"
    
    @asset(
        name=f"supabase_public_bookings_{tenant_name}",
        group_name=f"bookings_etl_{tenant_name}",
        metadata={
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "description": f"Asset representing the 'bookings' table in Supabase public schema for tenant {tenant_name}."
        },
        required_resource_keys={"supabase"},
        ins={
            "tokeet_datafeeds_bookings": AssetIn(AssetKey(tokeet_datafeeds_bookings_name)),
            "tokeet_datafeeds_rentals": AssetIn(AssetKey(tokeet_datafeeds_rentals_name))
        }
    
    )
    def supabase_public_bookings(context, tokeet_datafeeds_bookings, tokeet_datafeeds_rentals):
        # Transform the data
        upsert_dict = transform_supabase_public_bookings(tokeet_datafeeds_bookings, tokeet_datafeeds_rentals, tenant_id)
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
        try:
            supabase = context.resources.supabase
            context.log.info(f"Upsert data: {upsert_dict}")  # Log the upsert data
            if supabase_upsert(supabase, "bookings", upsert_dict):
                context.log.info(f"Upserted {len(upsert_dict)} rental records for tenant {tenant_name} ({tenant_id}).")
            else:
                error_message = f"Unknown error during upsert for tenant {tenant_name} ({tenant_id})"
                context.log.error(error_message)
                raise
        except Exception as e:
            error_message = f"Error during upsert for tenant {tenant_name} ({tenant_id}): {str(e)}"
            context.log.error(error_message)
            raise
        return Output(upsert_dict, metadata=metadata)
    
    return supabase_public_bookings

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
    bookings_df = bookings_df.replace({np.nan: None, "": None})

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
            bookings_df[col] = pd.to_datetime(bookings_df[col])
            bookings_df[col] = bookings_df[col].where(bookings_df[col].notna(), None).dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Replace empty strings with None for datetime columns
    bookings_df[datetime_columns] = bookings_df[datetime_columns].replace("", None)
    
    # Explicitly replace NaN values in all columns with None
    bookings_df = bookings_df.replace({pd.NaT: None, np.nan: None, "": None})
    
    # Custom function to handle NaN and empty string
    def replace_invalid(obj):
        if pd.isna(obj) or obj == "":
            return None
        return obj

    # Convert DataFrame to dictionary with custom handling for NaN and empty strings
    bookings_dict = json.loads(json.dumps(bookings_df.to_dict(orient='records'), default=replace_invalid))

    return bookings_dict