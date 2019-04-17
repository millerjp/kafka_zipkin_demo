#!/usr/bin/env bash

# Replace with your Autoscaling Group name
MY_ASG_NAME="KafkaDemoCluster-WebServerGroup"
TAG="kafka-brokers-demo"

sudo yum install -y jq
sudo yum install -y wget
sudo yum install -y java
cd ~
wget "http://archive.apache.org/dist/kafka/2.1.0/kafka_2.11-2.1.0.tgz"
tar -xvf kafka_2.11-2.1.0.tgz
cd kafka_2.11-2.1.0

INSTANCE_LAUNCH_ID=$(curl http://169.254.169.254/latest/meta-data/ami-launch-index)
MY_INST_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id)

MY_IP="UNDEFINED"

ZOOKEEPER_NODES=""
ZOOKEEPER_CONNECT=""
COUNTER=1
SERVER_I=0
ASSIGNED=0

eips=$(aws ec2 describe-addresses --region eu-central-1 --filter Name=tag-value,Values=$TAG)
for eip in $(echo "${eips}" | jq -rc '.Addresses[]'); do
    INST_ID=$(echo $eip | jq -r '.InstanceId')
    ALLOC=$(echo $eip | jq -r '.AllocationId')
    IP=$(echo $eip | jq -r '.PublicIp')
    if [[ $INST_ID == 'null' ]] && [[ $ASSIGNED == 0 ]]; then
        ASSIGNATION_FAILED=0
        aws ec2 associate-address --region eu-central-1 --allocation-id $ALLOC --instance-id $MY_INST_ID || ASSIGNATION_FAILED=1
        if [[ $ASSIGNATION_FAILED == 0 ]]; then
            ASSIGNED=1
            SERVER_I=$COUNTER
            MY_IP=$IP
            IP='0.0.0.0'
            echo "Assigned $IP"
        fi
    fi
    ZOOKEEPER_NODES=$ZOOKEEPER_NODES"server.$COUNTER=$IP:2888:3888"$'\n'
    if [ $COUNTER == 1 ]; then
        ZOOKEEPER_CONNECT="$IP:2181"
    else
        ZOOKEEPER_CONNECT="$ZOOKEEPER_CONNECT,$IP:2181"
    fi
    COUNTER=$(($COUNTER+1))
done

echo $ZOOKEEPER_NODES
echo $ZOOKEEPER_CONNECT

SERVER_ID=$(echo $MY_IP | tr . '\n' | awk '{s = s*256 + $1} END{print s}')

# Download default configuration templates
curl "https://raw.githubusercontent.com/ThinkportRepo/kafka_zipkin_demo/master/default_cfg/server.properties.default" > ./config/server.properties.temp
curl "https://raw.githubusercontent.com/ThinkportRepo/kafka_zipkin_demo/master/default_cfg/zookeeper.properties.default" > ./config/zookeeper.properties.temp

sed -i "s/#BROKER_ID#/$SERVER_I/g" ./config/server.properties.temp
sed -i "s/#ZOOKEEPER_CONNECT#/$ZOOKEEPER_CONNECT/g" ./config/server.properties.temp
sed -i "s/#ATVERTISED_LISTENERS#/PLAINTEXT:\/\/$MY_IP:9092/g" ./config/server.properties.temp

echo "$ZOOKEEPER_NODES" >> ./config/zookeeper.properties.temp

# Update actual config files
mv -f ./config/server.properties.temp ./config/server.properties
mv -f ./config/zookeeper.properties.temp ./config/zookeeper.properties

mkdir -p ~/logs
mkdir -p /tmp/zookeeper
# Set the zookeeper id
echo $(($SERVER_I)) > /tmp/zookeeper/myid
# Running 
./bin/zookeeper-server-start.sh ./config/zookeeper.properties > ~/logs/zookeeper.log 2>&1 &
./bin/kafka-server-start.sh ./config/server.properties > ~/logs/kafka.log 2>&1 &