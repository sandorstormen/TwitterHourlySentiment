#!/bin/bash
cat $1 | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:2181 --topic file
