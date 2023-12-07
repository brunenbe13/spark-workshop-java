#!/bin/bash

SPARK_HOME=""

${SPARK_HOME}/bin/spark-submit \
  --class "org.example.App" \
  --master "local[*]" \
  target/spark-workshop-java-1.0-SNAPSHOT.jar