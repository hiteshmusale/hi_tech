# bookings_etl/assets/supabase/supabase_public_bookings.py
from dagster import asset, Output, AssetIn, MetadataValue, AssetKey, Field, Int
from bookings_etl.utils import *
from bookings_etl.resources.supabase import *
import pandas as pd
import numpy as np
import uuid

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
        # Ensure bookings_df is a DataFrame
        if isinstance(supabase_public_bookings, list):
            bookings_df = pd.DataFrame(supabase_public_bookings)
        elif isinstance(supabase_public_bookings, pd.DataFrame):
            bookings_df = supabase_public_bookings
        else:
            raise ValueError("supabase_public_bookings should be a list or a DataFrame")
        
        # Discover missing_booking_values
        supabase = context.resources.supabase
        existing_booking_values = fetch_booking_value_ids(supabase, tenant_id)

        missing_bookings_df = bookings_df[~bookings_df["id"].isin(existing_booking_values)]
        context.log.info(f"Existing missing booking values: {missing_bookings_df}")

        # Filter out bookings without a rental name
        rental_missing_df = missing_bookings_df[missing_bookings_df['rental'].isnull() | (missing_bookings_df['rental'] == '')]
        missing_bookings_df = missing_bookings_df[missing_bookings_df['rental'].notnull() & (missing_bookings_df['rental'] != '')]

        # Append cleaning fees to the DataFrame
        rental_settings_df = fetch_rental_cleaning_fees_df(supabase, tenant_id)

        # Ensure rental_id consistency
        context.log.info(f"Missing bookings rental_ids: {missing_bookings_df['rental_id'].unique()}")
        context.log.info(f"Rental settings rental_ids: {rental_settings_df['rental_id'].unique()}")

        # Check for missing rental_ids before merge
        missing_rental_ids = set(missing_bookings_df['rental_id']) - set(rental_settings_df['rental_id'])
        if missing_rental_ids:
            context.log.warning(f"Missing rental settings for rental_ids: {missing_rental_ids}")

        # Perform the merge
        missing_bookings_df = missing_bookings_df.merge(rental_settings_df, how='left', on='rental_id')

        # Replace NaN values in fee_cleaning column with 0
        missing_bookings_df['fee_cleaning'].fillna(0, inplace=True)
        context.log.info(f"Appended fees_cleaning columns: {missing_bookings_df.head(5)}")
                
        # Create upsert list
        upsert_list = []
        
        for index, row in missing_bookings_df.iterrows():
            upsert = calculate_booking_values(row, tenant_id)
            upsert_list.append(upsert)
            
        context.log.info(f"Upsert list: {upsert_list}")
    
        # Log & prepare metadata
        context.log.info(f"Processed {len(upsert_list)} booking records for tenant {tenant_name} ({tenant_id}).")
        metadata = {
            "processed_booking_values": len(upsert_list),
            "bookings_missing_rental_ids": len(missing_rental_ids),
            "preview": upsert_list[:5]
        }
                
        # Check if upsert_list is empty before attempting the upsert
        if upsert_list:
            # Upsert to supabase
            try:
                if supabase_upsert(supabase, "booking_values", upsert_list):
                    context.log.info(f"Upserted {len(upsert_list)} booking_value records for tenant {tenant_name} ({tenant_id}).")
                else:
                    raise Exception(f"Failed to upsert booking_value records for tenant {tenant_name} ({tenant_id}).")
            except Exception as e:
                error_message = f"Error during booking_value upsert for tenant {tenant_name} ({tenant_id}): {str(e)}"
                context.log.error(error_message)
                raise
        else:
            context.log.info(f"No booking_value records to upsert for tenant {tenant_name} ({tenant_id})./n")
            
        return Output(upsert_list, metadata=metadata)
    return supabase_public_booking_values

def calculate_booking_values(booking: pd.Series, tenant_id: str) -> dict:
    """
    Calculate the booking values based on the provided booking information.

    Args:
        booking (pd.Series): A pandas Series object containing the booking information.
        tenant_id (str): The ID of the tenant.

    Returns:
        dict: A dictionary containing the calculated booking values rounded to 2DP.

    """
    id = str(uuid.uuid4())
    source = booking['source'].lower()
    tokeet_total = booking['total_cost']
    cleaning_fee = booking.get('fee_cleaning', 0)
    nights = booking.get('nights', 1)  # Default to 1 to avoid division by zero

    # Calculate gross amount based on source
    if source in ['airbnb', 'airbnbapiv2']:
        revenue_gross = tokeet_total / 0.82
        booking_fees_perc = 0.18
        card_fees_perc = 0
    elif source in ['booking.com', 'expedia', 'expedia.com']:
        revenue_gross = tokeet_total + cleaning_fee
        booking_fees_perc = 0.15
        card_fees_perc = 0.03
    else:  # Direct or other sources
        revenue_gross = tokeet_total
        booking_fees_perc = 0
        card_fees_perc = 0
        
    # Additional revenue calculations
    revenue_rate = revenue_gross - cleaning_fee
    booking_fees = revenue_gross * booking_fees_perc
    card_fees = revenue_gross * card_fees_perc
    revenue_payout = revenue_gross - booking_fees - card_fees
    
    # ADR calculations
    if nights > 0:
        adr_gross = revenue_gross / nights
        adr_rate = revenue_rate / nights
        adr_payout = revenue_payout / nights
    else:
        adr_gross = adr_rate = adr_payout = 0

    # Create a new dictionary with the calculated values
    upsert = {
        'id': id,
        'booking_id': booking['id'],
        'tenant_id': tenant_id,
        'revenue_gross': revenue_gross,
        'cleaning_fees': cleaning_fee,
        'revenue_rate': revenue_rate,
        'booking_fees': booking_fees,
        'card_fees': card_fees,
        'revenue_payout': revenue_payout,
        'adr_gross': adr_gross,
        'adr_rate': adr_rate,
        'adr_payout': adr_payout,
        'type': 'auto',
        'active': True
    }
    
    # Round all numerical values to 2 decimal places and convert to native Python types
    for key, value in upsert.items():
        if isinstance(value, (np.integer, np.int64)):
            upsert[key] = int(value)
        elif isinstance(value, (np.float64, float)):
            upsert[key] = round(float(value), 2)

    return upsert