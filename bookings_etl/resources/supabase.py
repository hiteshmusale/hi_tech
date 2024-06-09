# bookings_etl/resources/supabase.py
from supabase import create_client, Client
from dagster import resource
import os
from dotenv import load_dotenv

load_dotenv()

@resource
def supabase_resource(init_context) -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("Supabase URL or Key is missing from the environment variables")
    
    return create_client(url, key)

def fetch_tenant_settings(supabase: Client):
    response = supabase.table("tenant_settings_view").select("*").eq("active", True).eq("type", "etl_bookings").execute()
    return response.data
