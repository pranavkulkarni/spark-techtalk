#!/usr/bin/env bash
#  Run Spark locally with 2 worker threads in 2 cores (ideally, set this to the number of cores on your machine)
spark-submit --master local[2] \
 --driver-memory 10g \
 --jars $(echo ./lib/*.jar | tr ' ' ',') \
 --class $1 target/scala-2.11/spark-analysis_2.11-1.0.jar