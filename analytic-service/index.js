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
      console.log({
        topic,
        value: message.value.toString(),
      });
    },
  });
}

run().catch(console.error);
