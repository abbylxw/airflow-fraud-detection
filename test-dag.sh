#!/bin/bash
if [ -z "$1" ]; then
echo "Usage: ./test-dag.sh <dag_name.py>"
exit 1
fi
docker-compose exec airflow-webserver python /opt/airflow/dags/$1
