import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9092"], // must match exposed Docker port
});

const producer = kafka.producer();

const connectKafka = async () => {
  try {
    await producer.connect();
    console.log("Producer connected successfully");
  } catch (err) {
    console.error("Kafka connection error:", err);
  }
};

const app = express();
app.use(cors({ origin: "http://localhost:3000" }));
app.use(express.json());

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;
  const userId = "123";

  //todo payment

  //kafka

  await producer.send({
    topic: "payment-success",
    messages: [{ value: JSON.stringify({ userId, cart }) }],
  });

  await producer.send({
    topic: "payment-topic",
    messages: [{ value: JSON.stringify({ userId, cart }) }],
  });

  res.status(200).send("Payment successfully");
});

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(err.status || 500).send(err.message);
});

app.listen(8000, () => {
  connectKafka();
  console.log("Payment service is running on port 8000");
});
