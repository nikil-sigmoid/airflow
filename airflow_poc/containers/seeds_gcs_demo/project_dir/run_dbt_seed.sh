#!/bin/sh

jq .key encoded_gcs_sa.json | tr -d '\"' > encoded_gcs_sa_value.json
base64 -d encoded_gcs_sa_value.json > decoded_gcs_sa.json
gsutil -q -m -o Credentials:gs_service_key_file=decoded_gcs_sa.json cp -r gs://nr_example_seeds_bucket_1/seeds/* data
ls data
dbt seed --profiles-dir profile
dbt run --profiles-dir profile
#exit $?
#echo $?
#exit $?
#echo "Exit code: $?"