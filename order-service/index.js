import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({
  groupId: "order-service",
});

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topic: "payment-successful",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value?.toString();
      if (!value) return;

      const { userId, cart } = JSON.parse(value);

      const orderId = "1234";
      console.log(`Order consumer: Order created for the user ${userId}`);
      await producer.send({
        topic: "order-successful",
        messages: [{ value: JSON.stringify({ userId, cart, orderId }) }],
      });

      console.log(`📊 Analytics consumer: User ${userId} paid ${total}`);
    },
  });
}

run().catch(console.error);
