# SF Crime Project

- [SF Crime Project](#sf-crime-project)
  - [Overview](#overview)
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Performance Tunning Questions](#performance-tunning-questions)
    - [Question 1](#question-1)
    - [Question 2](#question-2)
  
## Overview

This project simulate the production, consumption and anlysis of SF crime statistics data using Apache Kafka and Apache Spark. Scripts include:

- `docker-compose.yaml`: Bring up a local Kafka cluster for simulation
- `kafka_server.py`: Simulate data to the Kafka brokers
- `kafka_consumer.py`: Consume data from using Kafka Consumer
- `spark_streaming.py`: Consume and analyse data using Spark Streaming

## Prerequisites

- Spark 2.4.5
- Scala 2.11.x
- Java 1.8.x
- Python 3.7
- Docker + Docker-compose

## Usage

1. Install dependencies

   Here I use `pipenv` to manage dependencies.

    ```bash
    # install pipenv
    pip install pipenv

    # install dependencies & enter virtualenv
    pipenv install
    pipenv shell
    ```

2. Unpack the data used for simulation into the current directory

    ```bash
    # if you have Make
    make unzip_data

    # which is equivalent to
    tar -zxvf data.tar.gz
    ```

3. Bring up the Kafka Cluster

    ```bash
    docker-compose up
    ```

4. Start simulating data by Kafka producer

   ```bash
   python kafka_server.py
   ```

5. Start Spark Streaming

    ```bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 spark_streaming.py
    ```

6. (Optional) Start consuming data with Kafka Consumer

    ```bash
    python consumer_server.py
    ```

## Performance Tunning Questions

### Question 1

> How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

It affects `processedRowsPerSecond`. Higher throughput and lower latency increase the `processedRowsPerSecond`, and vice versa.

### Question 2

> What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

The goal is to maximize `processedRowsPerSecond`.

To increase `processedRowsPerSecond`, these are the settings that are most efficient:

- `spark.default.parallelism`: total number of cores on all executor nodes.
- `spark.sql.shuffle.partitions`: number of partitions to use when shuffling data for joins or aggregations.
- `spark.streaming.kafka.maxRatePerPartition`: maximum rate at which data will be read from each Kafka partition.
