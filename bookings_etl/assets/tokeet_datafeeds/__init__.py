# bookings_etl/assets/supabase/tokeet_datafeeds.py

from dagster import asset, Output, MetadataValue
import pandas as pd
#from bookings_etl.resources.supabase import fetch_tenant_settings

# Define the function to fetch data from the data feed URL
def fetch_data_from_url(data_feed_url):
    """Fetch data from the data feed URL."""
    return pd.read_csv(data_feed_url)

def create_tokeet_datafeeds_rentals_asset(tenant_id, tenant_name, data_feed_url):
    """Factory function to create a rentals asset for a specific tenant."""
    @asset(
        name=f"tokeet_datafeeds_rentals_{tenant_name}",
        group_name="tokeet_datafeeds",
        metadata={
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "description": f"Tokeet datafeed of rentals data for tenant {tenant_name}."
        }
    )
    def tokeet_datafeeds_rentals(context):
        # Fetch data using the URL
        rentals_data = pd.read_csv(data_feed_url)
        
        # Log the number of records fetched
        context.log.info(f"Fetched {len(rentals_data)} rental records for tenant {tenant_name}.")
        
        # Prepare metadata
        metadata = {
            "num_rentals": len(rentals_data),
            "preview": MetadataValue.json(rentals_data.head().to_dict())  # Preview of the first 5 records
        }
        
        # Return the data as an Output with metadata
        return Output(rentals_data, metadata=metadata)
    
    return tokeet_datafeeds_rentals

"""
def create_tokeet_bookings_asset(tenant_id, data_feed_url):
    @asset(
        name=f"staging_tokeetdf_bookings_{tenant_id}",
        group_name="supabase_staging",
        metadata={
            "description": f"Staging table for Tokeet bookings data for tenant {tenant_id}."
        }
    )
    def staging_tokeetdf_bookings(context):
        # Fetch data using the URL
        bookings_data = pd.read_csv(data_feed_url)
        
        # Log the number of records fetched
        context.log.info(f"Fetched {len(bookings_data)} booking records for tenant {tenant_id}.")
        
        # Prepare metadata
        metadata = {
            "tenant_id": tenant_id,
            "num_bookings": len(bookings_data),
            "preview": MetadataValue.json(bookings_data.head().to_dict())  # Preview of the first 5 records
        }
        
        # Return the data as an Output with metadata
        return Output(bookings_data, metadata=metadata)
    
    return staging_tokeetdf_bookings

# Generate assets dynamically based on tenant settings
def generate_tenant_assets(supabase):
    tenant_settings = fetch_tenant_settings(supabase)
    
    assets = []
    for tenant in tenant_settings:
        tenant_id = tenant['tenant_id']
        rentals_url = tenant['json']['datafeed_rentals']
        bookings_url = tenant['json']['datafeed_bookings']
        
        rentals_asset = create_tokeet_rentals_asset(tenant_id, rentals_url)
        bookings_asset = create_tokeet_bookings_asset(tenant_id, bookings_url)
        
        assets.extend([rentals_asset, bookings_asset])
    
    return assets

# Example usage in your definitions
from dagster import Definitions
from bookings_etl.resources.supabase import supabase_resource

# Fetch assets dynamically
supabase_assets = generate_tenant_assets(supabase_resource)

defs = Definitions(
    assets=supabase_assets,
    resources={"supabase": supabase_resource}
)
"""