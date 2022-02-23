
# dbt seed demo

This project demonstrates uploading csv files to BigQuery from GCS bucket using **dbt seed** command.
## Usage/Examples

Go to your project directory in terminal
Encode your service account key using base64 and store it in key-value pair.

```bash
echo "{\"key\":\"`base64 gcs_sa_dbt.json`\"}" > encoded_gcs_sa.json
```

Build and push your image
```bash
docker build -t localhost:5000/dbt_seed_gcs:v1 .
docker push localhost:5000/dbt_seed_gcs:v1
```
Now just trigger your Airflow DAG, and you'll get your table created in BigQuery.