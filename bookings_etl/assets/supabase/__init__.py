# bookings_etl/assets/supabase/__init__.py
from dagster import asset, Output, MetadataValue
# bookings_etl/assets/supabase/__init__.py
from .supabase_public_rentals import create_supabase_public_rentals_asset

"""
# Define the asset for Supabase rentals table
@asset(
    required_resource_keys={"supabase"},
    group_name="supabase_public",
    metadata={
        "description": "Asset representing the 'rentals' table in Supabase public schema."
    }
)
def supabase_public_rentals(context):
    supabase = context.resources.supabase
    # Placeholder for the logic to fetch or interact with the rentals table
    rentals_data = supabase.fetch_table_data("rentals")
    context.log.info(f"Fetched {len(rentals_data)} rental records.")
    metadata = {"num_rentals": len(rentals_data)}
    return Output(rentals_data, metadata=metadata)

# Define the asset for Supabase bookings table
@asset(
    required_resource_keys={"supabase"},
    group_name="supabase_public",
    metadata={
        "description": "Asset representing the bookings table in Supabase."
    }
)
def supabase_public_bookings(context):
    supabase = context.resources.supabase
    # Placeholder for the logic to fetch or interact with the bookings table
    bookings_data = supabase.fetch_table_data("bookings")
    context.log.info(f"Fetched {len(bookings_data)} booking records.")
    metadata = {"num_bookings": len(bookings_data)}
    return Output(bookings_data, metadata=metadata)

# Define the asset for Supabase booking values table
@asset(
    required_resource_keys={"supabase"},
    group_name="supabase_public",
    metadata={
        "description": "Asset representing the booking values table in Supabase."
    }
)
def supabase_public_booking_values(context):
    supabase = context.resources.supabase
    # Placeholder for the logic to fetch or interact with the booking values table
    booking_values_data = supabase.fetch_table_data("booking_values")
    context.log.info(f"Fetched {len(booking_values_data)} booking value records.")
    metadata = {"num_booking_values": len(booking_values_data)}
    return Output(booking_values_data, metadata=metadata)
"""

"""
# Define the asset for Supabase rental performance table
@asset(
    required_resource_keys={"supabase"},
    group_name="supabase_public",
    metadata={
        "description": "Asset representing the rental performance table in Supabase."
    }
)
def supabase_public_rental_performance(context):
    supabase = context.resources.supabase
    # Placeholder for the logic to fetch or interact with the rental performance table
    rental_performance_data = supabase.fetch_table_data("rental_performance")
    context.log.info(f"Fetched {len(rental_performance_data)} rental performance records.")
    metadata = {"num_rental_performance": len(rental_performance_data)}
    return Output(rental_performance_data, metadata=metadata)
"""