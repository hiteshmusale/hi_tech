from dagster import Definitions, build_init_resource_context, ScheduleDefinition
from bookings_etl.jobs.materialize_assets import materialize_assets_job
from bookings_etl.resources.supabase import supabase_resource, fetch_tenant_settings
from bookings_etl.assets.tokeet_datafeeds import *
from bookings_etl.assets.supabase import *
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
    datafeed_url_rentals = tenant['json']['datafeed_rentals']
    datafeed_url_bookings = tenant['json']['datafeed_bookings']
    
    # Create Tokeet datafeeds rentals asset
    tokeet_rentals_def = create_tokeet_datafeeds_rentals_asset(tenant['tenant_id'], tenant_name, datafeed_url_rentals)
    all_assets.append(tokeet_rentals_def)
    
    # Create Tokeet datafeeds bookings asset
    tokeet_bookings_def = create_tokeet_datafeeds_bookings_asset(tenant['tenant_id'], tenant_name, datafeed_url_bookings)
    all_assets.append(tokeet_bookings_def)
    
    # Create Supabase public rentals assets
    supabase_rentals_def = create_supabase_public_rentals_asset(tenant['tenant_id'], tenant_name)
    all_assets.append(supabase_rentals_def)
    
    # Create Supabase public bookings asset
    supabase_bookings_def = create_supabase_public_bookings_asset(tenant['tenant_id'], tenant_name)
    all_assets.append(supabase_bookings_def)
    
    # Create Supabase public booking values asset
    supabase_booking_values_def = create_supabase_public_booking_values_asset(tenant['tenant_id'], tenant_name)
    all_assets.append(supabase_booking_values_def)
    
    # Create Supabase public reports view asset
    supabase_reports_view_def = create_supabase_public_reports_view_asset(tenant['tenant_id'], tenant_name)
    all_assets.append(supabase_reports_view_def)
    
    
# Define resources
resource_defs = {
    "supabase": supabase_resource
}

# Create a schedule that will run daily at midnight
materialize_all_schedule = ScheduleDefinition(
    job=materialize_assets_job,
    cron_schedule="0 0 * * *",
    name="daily_materialize_all",
)

materialize_test_schedule = ScheduleDefinition(
    job=materialize_assets_job,
    cron_schedule="0 1 * * *",
    name="test_daily_materialize_all",
)

# Define everything
defs = Definitions(
    assets=all_assets,
    resources=resource_defs,
    jobs=[materialize_assets_job],
    schedules=[materialize_all_schedule, materialize_test_schedule],
)
