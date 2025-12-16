import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "analytic-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "analytic-service",
});

async function run() {
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

      const total = cart.reduce((acc, item) => acc + item.price * (item.quantity || 1), 0);

      console.log(`📊 Analytics consumer: User ${userId} paid ${total}`);
    },
  });
}

run().catch(console.error);
