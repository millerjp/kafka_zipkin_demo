MAC=$(curl http://169.254.169.254/latest/meta-data/network/interfaces/macs/)
CIDR=$(curl http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/subnet-ipv4-cidr-block)
SUBNET_RANGE="10.0.0."
BASE_IP="${CIDR//\/*/}"
IP_STARTS_FROM="${BASE_IP/$SUBNET_RANGE/}"
INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/ami-launch-index)
CUR_IP_SLOT=$(($IP_STARTS_FROM+$INSTANCE_ID))

NUM_OF_INSTANCES=3
COUNTER=$NUM_OF_INSTANCES
ZOOKEPER_NODES=""
ZOOKEPER_CONNECT=""
until [  $COUNTER -lt 0 ]; do
    IIP=$(($IP_STARTS_FROM+$COUNTER))
    IP="$SUBNET_RANGE$IIP"
    ZOOKEPER_NODES=$ZOOKEPER_NODES"server.$COUNTER=$IP:2888:3888"$'\n'
    if [ $NUM_OF_INSTANCES == $COUNTER ]
    then
        ZOOKEPER_CONNECT="$IP:2181"
    else
        ZOOKEPER_CONNECT="$ZOOKEPER_CONNECT,$IP:2181"
    fi
    COUNTER=$(($COUNTER-1))
done
echo $ZOOKEPER_CONNECT
echo $ZOOKEPER_NODES

sed -i "s/broker.id=0/broker.id=$INSTANCE_ID/g" /home/ec2-user/kafka_2.11-2.1.0/config/server.properties
sed -i "s/localhost:2181/$ZOOKEPER_CONNECT/g" /home/ec2-user/kafka_2.11-2.1.0/config/server.properties
echo "$ZOOKEPER_NODES" >> /home/ec2-user/kafka_2.11-2.1.0/config/zookeeper.properties 2>&1

mkdir -p /home/ec2-user/logs
/home/ec2-user/kafka_2.11-2.1.0/bin/zookeeper-server-start.sh /home/ec2-user/kafka_2.11-2.1.0/config/zookeeper.properties > /home/ec2-user/logs/zookeeper.log 2>&1 &
/home/ec2-user/kafka_2.11-2.1.0/bin/kafka-server-start.sh /home/ec2-user/kafka_2.11-2.1.0/config/server.properties > /home/ec2-user/logs/kafka.log 2>&1 &