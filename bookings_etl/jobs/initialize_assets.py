# jobs/initialize_assets.py

from dagster import job, op
from bookings_etl.resources.supabase import supabase_resource, fetch_tenant_settings
from bookings_etl.assets.tokeet_datafeeds import create_tokeet_datafeeds_rentals_asset

@op(required_resource_keys={"supabase"})
def generate_assets(context):
    supabase = context.resources.supabase
    tenant_settings = fetch_tenant_settings(supabase)
    
    # Log the response from tenant settings
    context.log.info(f"Tenant settings response: {tenant_settings}")
    
    # Create tokeet assets
    tenant_settings_tokeet = filter(lambda t: t['json']['type'] == "tokeet_datafeed", tenant_settings)
    for tenant in tenant_settings_tokeet:
        tenant_name = tenant['tenant_name'].replace(" ", "_")
        data_feed_url = tenant['json']['datafeed_rentals']
        asset_def = create_tokeet_datafeeds_rentals_asset(tenant_name, data_feed_url)
        
        context.log.info(f"Generated asset for tenant {tenant_name}")
        
    # Create hostaway assets
    """etc"""

@job(resource_defs={"supabase": supabase_resource})
def initialize_bookings_assets():
    generate_assets()
