# bookings_etl/assets/tokeet_datafeeds/tokeet_datafeeds_rentals.py
from dagster import asset, Output, MetadataValue
import pandas as pd

def create_tokeet_datafeeds_rentals_asset(tenant_id, tenant_name, data_feed_url):
    """Factory function to create a rentals asset for a specific tenant."""
    @asset(
        name=f"tokeet_datafeeds_rentals_{tenant_name}",
        group_name=f"bookings_etl_{tenant_name}",
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