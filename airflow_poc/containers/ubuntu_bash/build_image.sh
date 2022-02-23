#!/bin/bash

docker build -t localhost:5000/ubuntu_bash:v1 .
docker push localhost:5000/ubuntu_bash:v1