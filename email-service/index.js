import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "email-service",
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
    topic: "email-successful",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value?.toString();
      if (!value) return;

      const { userId, cart } = JSON.parse(value);

      const dummyEmailId = "1234";

      await producer.send({
        topic: "order-successful",
        messages: [{ value: JSON.stringify({ userId, cart, dummyOrderId }) }],
      });

      console.log(`📊 Analytics consumer: User ${userId} paid ${total}`);
    },
  });
}

run().catch(console.error);
