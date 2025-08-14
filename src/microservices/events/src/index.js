const {Kafka} = require('kafkajs');
const express = require('express');
const bodyParser = require('body-parser');

const KAFKA_BROKERS = process.env['KAFKA_BROKERS']

const CREATE_MOVIE_EVENT = "movie-events"
const CREATE_USER_EVENT = "user-events"
const CREATE_PAYMENT_EVENT = "payment-events"

const kafka = new Kafka({
    clientId: 'events',
    brokers: [KAFKA_BROKERS] // Укажите ваши брокеры Kafka
});

const producer = kafka.producer();
const consumer = kafka.consumer({groupId: 'consumer'});

const app = express();
app.use(bodyParser.json());

const PORT = process.env['PORT'];

const sendEvent = async (event, data) => {
    await producer.send({
        topic: event,
        messages: [
            {value: JSON.stringify({...data, timestamp: new Date().toISOString()})},
        ],
    });
}

async function startProducer() {
    await producer.connect();
    console.log('Kafka Producer connected');
}

async function startConsumer() {
    await consumer.connect();
    await consumer.subscribe({topic: CREATE_MOVIE_EVENT});
    await consumer.subscribe({topic: CREATE_USER_EVENT});
    await consumer.subscribe({topic: CREATE_PAYMENT_EVENT});

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            console.log({
                topic,
                partition,
                value: message.value.toString(),
            });
        },
    });

    console.log('Kafka Consumer is running and listening to topics');
}

app.post('/api/events/user', async (req, res) => {
    try {
        const {user_id, username, action, timestamp} = req.body;

        if (!(user_id && username && action && timestamp)) {
            return res.status(400).json({error: 'user_id, username, action, timestamp are required'});
        }

        await sendEvent(CREATE_USER_EVENT, req.body)

        res.status(201).json({status: 'success'});
    } catch (error) {
        res.status(500).send();
    }
});

app.post('/api/events/movie', async (req, res) => {
    try {
        const {movie_id, title, action, user_id} = req.body;

        if (!(movie_id && title && action && user_id)) {
            return res.status(400).json({error: 'movie_id, title, action, user_id are required'});
        }

        await sendEvent(CREATE_MOVIE_EVENT, req.body)

        res.status(201).json({status: 'success'});
    } catch (error) {
        res.status(500).send();
    }
});


app.post('/api/events/payment', async (req, res) => {
    try {
        const {payment_id, user_id, amount, status, timestamp, method_type} = req.body;

        if (!(payment_id && user_id && amount && status && timestamp && method_type)) {
            return res.status(400).json({error: 'payment_id, user_id, amount, status, timestamp are required'});
        }

       await sendEvent(CREATE_PAYMENT_EVENT, req.body)

        res.status(201).json({status: 'success'});
    } catch (error) {
        res.status(500).send();
    }
});

app.get('/api/events/health', (req, res) => {
    res.status(200).send();
});

// Запуск сервера и Kafka клиентов
async function start() {

    await startProducer();
    await startConsumer();

    app.listen(PORT, () => {
        console.log(`server lister ${PORT}`);
    });

}

start();
