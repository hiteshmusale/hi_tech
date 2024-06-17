# bookings_etl/resources/supabase.py
from supabase import create_client, Client
from dagster import resource
import os
from dotenv import load_dotenv
import pandas as pd
import json

load_dotenv()

@resource
def supabase_resource(init_context) -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("Supabase URL or Key is missing from the environment variables")
    
    return create_client(url, key)

def fetch_tenant_settings(supabase: Client):
    response = supabase.table("tenant_settings_view").select("*").eq("type", "etl_bookings").execute()
    return response.data

def fetch_rental_cleaning_fees_df(supabase: Client, tenant_id: str) -> pd.DataFrame:
    response = supabase.table("rental_settings").select("rental_id", "fee_cleaning").eq("tenant_id", tenant_id).execute()
    #if response.error:
    #    raise Exception(f"Error fetching rental settings: {response.error.message}")
    
    return pd.DataFrame(response.data)

def supabase_upsert(supabase: Client, table: str, upsert_data: list) -> bool:
    # Cleanse the data before upserting
    upsert_data = replace_none_with_empty_string(upsert_data)
    
    key_columns = ['id']
    upsert_data = remove_duplicates(upsert_data, key_columns)
    
    print(f"Upserting to {table}: {upsert_data}")
    
    # Upsert to Supabase or return error message
    try:
        query = supabase.table(table).upsert(upsert_data)
        result = query.execute()
        return bool(result)
    except Exception as e:
        print(f"Error upserting data: {e}")
        raise

def fetch_booking_value_ids(supabase: Client, tenant_id: str) -> list:
    # TODO Add error handling again
    response = supabase.table("booking_values").select("booking_id").eq("tenant_id", tenant_id).execute()
    booking_ids = [record["booking_id"] for record in response.data if "booking_id" in record]
    return booking_ids

def replace_none_with_empty_string(data):
    ignore_fields = ['id', 'tenant_id', 'booking_id', 'rental_id', 'bedrooms', 'bathrooms', 'sleep_max', 'arrive', 'depart', 'received']
    
    if isinstance(data, dict):
        return {k: ("" if v is None and k not in ignore_fields else (None if v is None and k in ignore_fields else v)) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_none_with_empty_string(item) for item in data]
    else:
        return data

# Identify duplicates based on constrained columns
def remove_duplicates(upsert_data, key_columns):
    seen = set()
    unique_data = []
    for item in upsert_data:
        key = tuple(item[col] for col in key_columns)
        if key not in seen:
            seen.add(key)
            unique_data.append(item)
    return unique_data