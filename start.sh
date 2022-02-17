#!/bin/bash

echo "+------------------+"
echo "| START CONTAINERS |"
echo "+------------------+"
echo ""

podman-compose up -d

echo "+------------------+"
echo "| UPLOAD JARS      |"
echo "+------------------+"
echo ""

SPARK_MASTER_CONTAINER=$(podman ps | grep _spark_ | awk '{print $1}')
podman exec -it ${SPARK_MASTER_CONTAINER} mkdir /opt/app
podman cp market-data-loader/target/market-data-loader-jar-with-dependencies.jar ${SPARK_MASTER_CONTAINER}:/opt/app
podman cp market-data-loader/conf/log4j.properties ${SPARK_MASTER_CONTAINER}:/opt/app
podman cp market-data-loader/conf/mdl.properties ${SPARK_MASTER_CONTAINER}:/opt/app
echo "Spark Master (${SPARK_MASTER_CONTAINER}) /opt/app:"
podman exec -it ${SPARK_MASTER_CONTAINER} ls -l /opt/app

SPARK_WORKER_1_CONTAINER=$(podman ps | grep spark-worker-1 | awk '{print $1}')
podman exec -it ${SPARK_WORKER_1_CONTAINER} mkdir /opt/app
podman cp market-data-loader/target/market-data-loader-jar-with-dependencies.jar ${SPARK_WORKER_1_CONTAINER}:/opt/app
podman cp market-data-loader/conf/log4j.properties ${SPARK_WORKER_1_CONTAINER}:/opt/app
podman cp market-data-loader/conf/mdl.properties ${SPARK_WORKER_1_CONTAINER}:/opt/app
echo "Spark Worker 1 (${SPARK_WORKER_1_CONTAINER}) /opt/app:"
podman exec -it ${SPARK_WORKER_1_CONTAINER} ls -l /opt/app

SPARK_WORKER_2_CONTAINER=$(podman ps | grep spark-worker-2 | awk '{print $1}')
podman exec -it ${SPARK_WORKER_2_CONTAINER} mkdir /opt/app
podman cp market-data-loader/target/market-data-loader-jar-with-dependencies.jar ${SPARK_WORKER_2_CONTAINER}:/opt/app
podman cp market-data-loader/conf/log4j.properties ${SPARK_WORKER_2_CONTAINER}:/opt/app
podman cp market-data-loader/conf/mdl.properties ${SPARK_WORKER_2_CONTAINER}:/opt/app
echo "Spark Worker 2 (${SPARK_WORKER_2_CONTAINER}) /opt/app:"
podman exec -it ${SPARK_WORKER_2_CONTAINER} ls -l /opt/app

echo "+------------------+"
echo "| INIT DATABASES   |"
echo "+------------------+"
echo ""

TRADESDB_CONTAINER=$(podman ps | grep tradesdb | awk '{print $1}')

echo "TRADESDB (${TRADESDB_CONTAINER}):"

podman cp init_tradesdb/init_tradesdb.sql ${TRADESDB_CONTAINER}:/docker-entrypoint-initdb.d/init_tradesdb.sql
podman cp init_tradesdb/trades_small.csv ${TRADESDB_CONTAINER}:/docker-entrypoint-initdb.d/trades_small.csv
podman cp init_tradesdb/init.sh ${TRADESDB_CONTAINER}:/docker-entrypoint-initdb.d/init.sh
podman exec -it ${TRADESDB_CONTAINER} chmod 755 docker-entrypoint-initdb.d/init.sh
podman exec -it ${TRADESDB_CONTAINER} docker-entrypoint-initdb.d/init.sh

SERVINGDB_CONTAINER=$(podman ps | grep servingdb | awk '{print $1}')

echo "SERVINGDB (${SERVINGDB_CONTAINER}):"

podman cp init_servingdb/init_servingdb.sql ${SERVINGDB_CONTAINER}:/docker-entrypoint-initdb.d/init_servingdb.sql
podman cp init_servingdb/init.sh ${SERVINGDB_CONTAINER}:/docker-entrypoint-initdb.d/init.sh
podman exec -it ${SERVINGDB_CONTAINER} chmod 755 docker-entrypoint-initdb.d/init.sh
podman exec -it ${SERVINGDB_CONTAINER} docker-entrypoint-initdb.d/init.sh
