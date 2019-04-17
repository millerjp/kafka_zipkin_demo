#!/usr/bin/env bash
echo "erwrre" > /tmp/rax.log
/home/ec2-user/kafka_2.11-2.1.0/bin/zookeeper-server-start.sh /home/ec2-user/kafka_2.11-2.1.0/config/zookeeper.properties > /tmp/zoo.log 2>&1 &
/home/ec2-user/kafka_2.11-2.1.0/bin/kafka-server-start.sh /home/ec2-user/kafka_2.11-2.1.0/config/server-1.properties > /tmp/broker1.log 2>&1 &
/home/ec2-user/kafka_2.11-2.1.0/bin/kafka-server-start.sh /home/ec2-user/kafka_2.11-2.1.0/config/server-2.properties > /tmp/broker2.log 2>&1 &
echo "32423424" > /tmp/rax2.log