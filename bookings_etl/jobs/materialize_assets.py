# jobs/materialize_assets.py
from dagster import job, AssetSelection, materialize

@job(selection=AssetSelection.all())
def materialize_assets_job():
    materialize()