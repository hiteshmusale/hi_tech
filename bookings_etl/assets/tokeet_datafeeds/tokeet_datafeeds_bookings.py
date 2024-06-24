# bookings_etl/assets/tokeet_datafeeds/tokeet_datafeeds_bookings.py
from dagster import asset, Output, MetadataValue
import pandas as pd

def create_tokeet_datafeeds_bookings_asset(tenant_id, tenant_name, data_feed_url):
    """Factory function to create a bookings asset for a specific tenant."""
    @asset(
        name=f"tokeet_datafeeds_bookings_{tenant_name}",
        group_name=f"bookings_etl_{tenant_name}",
        metadata={
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "description": f"Tokeet datafeed of bookings data for tenant {tenant_name}."
        }
    )
    def tokeet_datafeeds_bookings(context):
        # Fetch data using the URL
        bookings_df = get_bookings(data_feed_url)
        
        # Log the number of records fetched
        context.log.info(f"Fetched {len(bookings_df)} bookings records for tenant {tenant_name}.")
        
        # Prepare metadata
        metadata = {
            "num_bookings": len(bookings_df),
            "preview": MetadataValue.json(bookings_df.head().to_dict())  # Preview of the first 5 records
        }
        
        # Return the data as an Output with metadata
        return Output(bookings_df, metadata=metadata)
    
    return tokeet_datafeeds_bookings

def get_bookings(data_feed_url):
    """Return all bookings from the data feed."""
    bookings_df = pd.DataFrame()
    temp = pd.DataFrame()
    skip = 0
    limit = 1000

    while True:
        link = f"{data_feed_url}?sort=guest_arrive&direction=1&skip={skip}&limit={limit}"
        temp_data = pd.read_csv(link)

        if temp_data.shape == (2, 1):
            break
        temp = pd.concat([temp, temp_data])
        skip += limit

    bookings_df = pd.concat([bookings_df, temp])
    
    filtered_bookings_df = bookings_df[bookings_df["Booking Status"].isin(["Booked", "Cancelled"])]

    return filtered_bookings_df
