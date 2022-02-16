#!/usr/bin/env bash

export $(xargs -0 -a "/proc/1/environ")

/opt/bitnami/spark/bin/spark-submit \
  --class io.github.tankist88.mdle.mdl.Main \
  --name "$1" \
  --master spark://spark:7077 \
  --deploy-mode "$5" \
  --supervise \
  --executor-memory "$2" \
  --driver-memory "$3" \
  --num-executors "$4" \
  --driver-java-options "-Dlog4j.configuration=file:/opt/app/log4j.properties -Dmdl.config.path=/opt/app/mdl.properties" \
  --files /opt/app/mdl.properties,/opt/app/log4j.properties \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.standalone.submit.waitAppCompletion=false" \
  --conf "spark.driver.host=mdl-batch" \
  --conf "spark.ui.port=4042" \
  --conf "spark.cores.max=1" \
  /opt/app/market-data-loader-jar-with-dependencies.jar \
  "$1" "$5"