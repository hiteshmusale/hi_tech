# bookings_etl/assets/supabase/supabase_public_reports_view.py
from dagster import (
    asset,
    Output,
    AssetIn,
    MetadataValue,
    AssetKey,
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetCheckSeverity,
    Field,
    Int
)
from bookings_etl.utils import *
from bookings_etl.resources.supabase import *
import pandas as pd
import numpy as np

def create_supabase_public_reports_view_asset(tenant_id, tenant_name):
    """Factory function to create a reports_view asset for a specific tenant."""
    
    supabase_public_booking_values_name = f"supabase_public_booking_values_{tenant_name}"
    supabase_public_reports_view_name = f"supabase_public_reports_view_{tenant_name}"
    
    @asset(
        name=f"supabase_public_reports_view_{tenant_name}",
        group_name=f"bookings_etl_{tenant_name}",
        metadata={
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "description": f"Asset representing the 'reports_view' table in Supabase public schema for tenant {tenant_name}."
        },
        required_resource_keys={"supabase"},
        ins={"supabase_public_booking_values": AssetIn(AssetKey(supabase_public_booking_values_name))},
        check_specs=[
            AssetCheckSpec(name="df_not_empty", asset=supabase_public_reports_view_name),
            AssetCheckSpec(name="kpis_not_empty", asset=supabase_public_reports_view_name),
            AssetCheckSpec(name="kpis_not_negative", asset=supabase_public_reports_view_name),
            AssetCheckSpec(name="overbooked", asset=supabase_public_reports_view_name),
        ]
    )
    def supabase_public_reports_view(context: AssetExecutionContext, supabase_public_booking_values):
        # Trigger refresh reports
        #
        
        # Load reports_view_df
        supabase = context.resources.supabase
        reports_view_df = fetch_reports_view_df(supabase, tenant_id)
        
        # Set metadata and output
        metadata = {
            "reports_count": len(reports_view_df),
        }
        yield Output(reports_view_df, metadata=metadata)
        
        # Asset checks
        yield check_df_not_empty(reports_view_df)
        yield check_kpis_not_empty(reports_view_df)
        yield check_kpis_not_negative(reports_view_df)
        yield check_overbooked(reports_view_df)
        
    return supabase_public_reports_view

def check_df_not_empty(df):
    return AssetCheckResult(
        check_name="df_not_empty",
        passed=bool(len(df) > 0),
        metadata={
            "row_count": len(df)
        }
    )

def check_kpis_not_empty(df):
    # List of KPI columns to check
    kpi_columns = ['actuals_id', 'actuals', 'forecasts_id', 'forecasts']
    
    # Check for null or empty values in KPI columns
    total_null_counts = df[kpi_columns].isnull().sum().sum()
    total_empty_counts = (df[kpi_columns] == '').sum().sum()
    
    # Create the AssetCheckResult
    return AssetCheckResult(
        check_name="kpis_not_empty",
        passed=bool(total_null_counts == 0 and total_empty_counts == 0),
        metadata={
            "checked_columns": kpi_columns,
            "total_null_counts": int(total_null_counts),
            "total_empty_counts": int(total_empty_counts)
        }
    )
    
def check_kpis_not_negative(df):
    # List of KPI columns to check for negative values
    kpi_columns = ['actuals', 'forecasts']
    
    # Initialize counters for negative values
    rows_with_negative_actuals = set()
    rows_with_negative_forecasts = set()
    rows_with_negatives = pd.DataFrame()

    # Check for negative values in each KPI column
    for kpi in kpi_columns:
        if kpi in df.columns:
            for index, value in df[kpi].dropna().items():
                if isinstance(value, dict):
                    # Find negative values in the dictionary
                    if any(v < 0 for v in value.values()):
                        if kpi == 'actuals':
                            rows_with_negative_actuals.add(index)
                        elif kpi == 'forecasts':
                            rows_with_negative_forecasts.add(index)
                        # Add the row with negative values to the DataFrame
                        rows_with_negatives = pd.concat([rows_with_negatives, df.loc[[index]]])

    total_negative_actuals = len(rows_with_negative_actuals)
    total_negative_forecasts = len(rows_with_negative_forecasts)

    # Convert the rows with negatives to JSON
    rows_with_negatives_json = rows_with_negatives.to_json(orient="records")

    # Create the AssetCheckResult
    return AssetCheckResult(
        check_name="kpis_not_negative",
        passed=bool(total_negative_actuals == 0 and total_negative_forecasts == 0),
        metadata={
            "checked_columns": kpi_columns,
            "rows_with_negative_actuals": total_negative_actuals,
            "rows_with_negative_forecasts": total_negative_forecasts,
            "rows_with_negatives": rows_with_negatives_json
        }
    )
    
def check_overbooked(df):
    # Initialize counter for overbooked rows
    rows_overbooked_indices = set()
    rows_overbooked = pd.DataFrame()

    # Check for overbooked rows in the actuals column
    if 'actuals' in df.columns:
        for index, value in df['actuals'].dropna().items():
            if isinstance(value, dict):
                nights_available_gross = value.get('nights_available_gross')
                nights_booked = value.get('nights_booked')
                if nights_available_gross is not None and nights_booked is not None:
                    if nights_booked > nights_available_gross:
                        rows_overbooked_indices.add(index)
                        # Add the row with overbooked values to the DataFrame
                        rows_overbooked = pd.concat([rows_overbooked, df.loc[[index]]])

    total_overbooked_count = len(rows_overbooked_indices)

    # Convert the rows with overbooked values to JSON
    rows_overbooked_json = rows_overbooked.to_json(orient="records")

    # Create the AssetCheckResult
    return AssetCheckResult(
        check_name="overbooked",
        passed=total_overbooked_count == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "total_overbooked_count": total_overbooked_count,
            "rows_overbooked": rows_overbooked_json
        }
    )