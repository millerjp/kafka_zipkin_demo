// @flow

var kafka = require('kafka-node'),
    Producer = kafka.Producer;
const client = new kafka.KafkaClient({kafkaHost: 'Kafka-Appli-LYHBPY9HL8RM-778f38d422e62529.elb.eu-central-1.amazonaws.com:9092'});

const Consumer = kafka.Consumer;
const consumer = new Consumer(
    client,
    [
        { topic: 't1', partition: 1 }
    ],
    {
        autoCommit: false
    }
);

consumer.on('message', function (message) {
    console.log(message);
});

// const producer = new Producer(client);

// const payloads = [
//     { topic: 'topic1', messages: 'hi', partition: 0 },
//     { topic: 'topic2', messages: ['hello', 'world'] }
// ];
// producer.on('ready', function () {
//     producer.send(payloads, function (err, data) {
//         console.log(data);
//     });
// });

// producer.on('error', console.log);