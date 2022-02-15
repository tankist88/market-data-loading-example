#!/usr/bin/env bash

/opt/bitnami/spark/bin/spark-submit \
  --class io.github.tankist88.mdle.mdl.Main \
  --name "$1" \
  --master spark://spark:7077 \
  --deploy-mode "$5" \
  --supervise \
  --executor-memory "$2" \
  --driver-memory "$3" \
  --num-executors "$4" \
  --driver-java-options "-Dlog4j.configuration=file:log4j.properties" \
  --files mdl.properties,log4j.properties \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.standalone.submit.waitAppCompletion=false" \
  --conf "spark.streaming.kafka.maxRatePerPartition=500" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  --conf "spark.driver.host=mdl" \
  market-data-loader-jar-with-dependencies.jar \
  "$1" "$5"