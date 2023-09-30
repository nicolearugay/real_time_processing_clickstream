# Real Time Processing of Clickstream Data 

## Table of Contents

- [Overview](#overview)
- [Technologies Used](#technologies-used)
- [Data Flow Design](#data-flow-design)
- [Stream Processing with Spark](#stream-processing-with-spark)
- [Batch Processing and Storage with Snowflake](#batch-processing-and-storage-with-snowflake)
- [Monitoring, Logging, and Alerts](#monitoring-logging-and-alerts)

## Overview

This project showcases end-to-end data engineering tasks ranging from data simulation to real-time and batch processing.

## Technologies Used 
Python   
SQL (Snowflake)  
Apache Spark   
Apache Kafka   
Prometheus   
Kafka Exporter   
Grafana   
Docker   
Confluent  

## Data Flow Design

<img src="/screenshots/architecture.png" alt="Alt text" width="750"/>  

## Data Ingestion with Kafka

### Setting Up a Kafka Broker
Following confluent's basic set up with Python and using a local cluster. 

1. Prerequisites    
   a. Linux    
   b. Python    
2. Create project  
   a. Create a new directory  
   b. Create and activate a Python virtual environment  
   c. Install the following libraries  
```bash
pip install confluent-kafka configparser
```
3. Create a docker-compose.yml file and start it. See 'docker-compose.yml'.  
4. Create an ini file. See 'example.ini'  
5. Create a topic called 'clickstream'.   
```bash
docker compose exec broker \
  kafka-topics --create \
    --topic clickstream \
    --bootstrap-server broker:9092 \
    --replication-factor 1 \
    --partitions 1
```

## Stream Processing with Spark

1. Build producer. See 'clickstream_producer.py'.   
2. Build consumer. See 'clickstream_consumer.py'.  
3. Produce events.  
```bash
chmod u+x producer.py
./clickstream_producer.py getting_started.ini
```
Output looks like the following:   
```bash
Produced record to clickstream
Produced record to clickstream
Produced record to clickstream
Produced record to clickstream
Produced record to clickstream
```
4. In another terminal, consume events.  
```bash
chmod u+x consumer.py
./consumer.py getting_started.ini
```
Output:   
```bash
Consumed event from topic clickstream: user_id = 755, page = cart, action = click
Consumed event from topic clickstream: user_id = 158, page = cart, action = view
Consumed event from topic clickstream: user_id = 358, page = homepage, action = purchase
```


## Batch Processing and Storage with Snowflake

### Snowflake Setup

Create tables in Snowflake to hold both raw and enriched data. 

### Spark Streaming Job

Create a Spark job that batch processes to Snowflake and run it. See 'spark_streaming_job.py'. Here we're using foreachBatch to call the function. 

### Data Transformation 

We're transforming in real-time by deserializing JSON data and transforming the data to create a new DataFrame with a flattened structure. 

### Real-Time Data Enrichmenet 

In the process_batch() function, we're writing a raw table and an enriched table to Snowflake.  


## Monitoring, Logging, and Alerts

### Setup Logging in Job Script

Implement logging in the batch processing script.

### Monitoring Script with Alerts

Set up a monitoring script that triggers alerts based on defined conditions. See 'monitoring.py'. Alerts are written to the terminal. Email alerts can be added.

### Kafka Exporter + Prometheus

Configure Kafka Exporter and Prometheus for monitoring Kafka metrics.

### Grafana

Set up Grafana to visualize the metrics from Prometheus.

