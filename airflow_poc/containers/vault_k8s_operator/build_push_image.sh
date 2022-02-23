#!/bin/bash

docker build -t localhost:5000/vault_k8s_operator:v1 .
docker push localhost:5000/vault_k8s_operator:v1
cd /Users/nik/Desktop/my_projects/airflow/airflow_poc
airflow dags trigger vault_k8s_operator