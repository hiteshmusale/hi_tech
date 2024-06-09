from dagster import Definitions, build_init_resource_context, AssetKey
#from bookings_etl.jobs.initialize_assets import initialize_bookings_assets
#from bookings_etl.jobs.materialize_assets import materialize_assets_job
from bookings_etl.resources.supabase import supabase_resource, fetch_tenant_settings
from bookings_etl.assets.tokeet_datafeeds import create_tokeet_datafeeds_rentals_asset
from bookings_etl.assets.supabase import create_supabase_public_rentals_asset
import os
from dotenv import load_dotenv

# Optionally load environment variables if not handled by the deployment environment
if os.getenv("ENVIRONMENT") != "production":
    load_dotenv()

# Initialize Supabase resource
init_context = build_init_resource_context(config={
    "url": os.getenv("SUPABASE_URL"),
    "key": os.getenv("SUPABASE_KEY")
})
supabase = supabase_resource(init_context)

# Fetch tenant settings using the initialized Supabase resource
tenant_settings = fetch_tenant_settings(supabase)

# Generate asset definitions based on tenant settings
all_assets = []
for tenant in tenant_settings:
    tenant_name = tenant['tenant_name'].replace(" ", "_")
    data_feed_url = tenant['json']['datafeed_rentals']
    
    tokeet_asset_def = create_tokeet_datafeeds_rentals_asset(tenant['tenant_id'], tenant_name, data_feed_url)
    all_assets.append(tokeet_asset_def)
    
    # Create Supabase public rentals assets, passing the asset definition directly
    supabase_asset_def = create_supabase_public_rentals_asset(tenant['tenant_id'], tenant_name)
    all_assets.append(supabase_asset_def)

# Define resources
resource_defs = {
    "supabase": supabase_resource
}

# Define everything
defs = Definitions(
    assets=all_assets,
    resources=resource_defs,
    #jobs=[materialize_assets_job, initialize_bookings_assets],
)
