# jobs/materialize_assets.py
from dagster import AssetSelection, define_asset_job
    
materialize_assets_job = define_asset_job("materialize_all_job", selection=AssetSelection.all())
