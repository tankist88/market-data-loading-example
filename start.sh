#!/bin/bash

echo "+------------------+"
echo "| START CONTAINERS |"
echo "+------------------+"
echo ""

podman-compose up -d

echo "+------------------+"
echo "| INIT DATABASES   |"
echo "+------------------+"
echo ""

TRADESDB_CONTAINER=$(podman ps | grep tradesdb | awk '{print $1}')

echo "TRADESDB CONTAINER: ${TRADESDB_CONTAINER}"

podman cp init_tradesdb/init_tradesdb.sql ${TRADESDB_CONTAINER}:/docker-entrypoint-initdb.d/init_tradesdb.sql
podman cp init_tradesdb/trades_small.csv ${TRADESDB_CONTAINER}:/docker-entrypoint-initdb.d/trades_small.csv
podman cp init_tradesdb/init.sh ${TRADESDB_CONTAINER}:/docker-entrypoint-initdb.d/init.sh
podman exec -it ${TRADESDB_CONTAINER} chmod 755 docker-entrypoint-initdb.d/init.sh
podman exec -it ${TRADESDB_CONTAINER} docker-entrypoint-initdb.d/init.sh

SERVINGDB_CONTAINER=$(podman ps | grep servingdb | awk '{print $1}')

echo "SERVINGDB CONTAINER: ${SERVINGDB_CONTAINER}"

podman cp init_servingdb/init_servingdb.sql ${SERVINGDB_CONTAINER}:/docker-entrypoint-initdb.d/init_servingdb.sql
podman cp init_servingdb/init.sh ${SERVINGDB_CONTAINER}:/docker-entrypoint-initdb.d/init.sh
podman exec -it ${SERVINGDB_CONTAINER} chmod 755 docker-entrypoint-initdb.d/init.sh
podman exec -it ${SERVINGDB_CONTAINER} docker-entrypoint-initdb.d/init.sh

