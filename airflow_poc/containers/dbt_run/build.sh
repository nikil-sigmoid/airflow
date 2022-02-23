#!/bin/bash

docker build -t dbt_run .
docker image tag dbt_run localhost:5000/dbt_run:v1
docker push localhost:5000/dbt_run:v1
