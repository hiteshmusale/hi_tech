# bookings_etl/assets/supabase/supabase_public_rentals.py
from dagster import asset, Output, MetadataValue, AssetKey

def create_supabase_public_rentals_asset(tenant_id, tenant_name):
    """Factory function to create a rentals asset for a specific tenant."""
    
    tokeet_datafeeds_rentals_name = f"tokeet_datafeeds_rentals_{tenant_name}"
    
    @asset(
        name=f"supabase_public_rentals_{tenant_name}",
        group_name="supabase_public",
        metadata={
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "description": f"Asset representing the 'rentals' table in Supabase public schema for tenant {tenant_name}."
        },
        required_resource_keys={"supabase"},
        deps=[AssetKey(tokeet_datafeeds_rentals_name)]
    )
    def supabase_public_rentals(context):
        tokeet_datafeeds_rentals_name = f"tokeet_datafeeds_rentals_{tenant_name}"
        tokeet_datafeeds_rentals = AssetKey(tokeet_datafeeds_rentals_name)
        context.log.info(f"Processed {len(tokeet_datafeeds_rentals)} rental records for tenant {tenant_name}.")
        metadata = {
            "num_rentals": len(tokeet_datafeeds_rentals),
            "preview": print(tokeet_datafeeds_rentals)
        }
        return Output(tokeet_datafeeds_rentals, metadata=metadata)
    
    return supabase_public_rentals


def transform_rentals_data(tokeet_datafeeds_rentals):
    # Placeholder transformation logic, adjust as needed
    # Example: convert column names, filter rows, etc.
    transformed_data = tokeet_datafeeds_rentals.copy()
    
    # Example transformation: ensuring IDs are valid UUIDs
    transformed_data['id'] = transformed_data['id'].apply(lambda x: str(x).lower())
    
    # Add any other transformation logic needed
    return transformed_data