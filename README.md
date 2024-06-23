# bookings_etl

This is the main bookings and rental ETL codebase for Short Stay AI.  Supported PMS:
- Tokeet
- Hostaway (Soon!)

## How it works

Each tenant has it's own pipeline, as defined in supabase[tenant_settings_json] with type = etl_bookings in this json format:

```json
{
    "type":"tokeet_datafeed",
    "datafeed_bookings":"url",
    "datafeed_rentals":"url"
}
```

The bookings_etl module loops through each tenant setting and create assets using the asset factories.

## Unit testing

Unit tests not yet in place: :o

## Editing the codebase

1. Download this repo
2. Create venv
```bash
python -m venv venv
source venv/bin/activate
```
3. Install poetry
```bash
pip install poetry
```
4. Install dependencies using poetry
```bash
poetry install
```
5. Run dagster locally
3. Install poetry
```bash
dagster dev -w workspace.yaml
```

## Deploy on Dagster Cloud

Currently via dagster CLI, but really need to setup github actions so that updates to main are automatically deployed.

1. Login to dagster-cloud CLI
```bash
dagster-cloud config setup
```
2. Deploy to prod using this command: 
```bash
dagster-cloud serverless deploy-python-executable --location-name bookings-etl --package-name bookings_etl --python-version=3.11
```
3. Cry when it doesn't work.