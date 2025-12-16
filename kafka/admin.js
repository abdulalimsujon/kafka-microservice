import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "kafka-service",
  brokers: ["localhost:9092"],
});

const admin = kafka.admin();

const run = async () => {
  try {
    await admin.connect();

    const existingTopics = await admin.listTopics();

    const topicsToCreate = ["payment-successful", "order-successful", "email-successful"].filter(
      (topic) => !existingTopics.includes(topic),
    );

    if (topicsToCreate.length === 0) {
      console.log("✅ Topics already created");
      return;
    }

    await admin.createTopics({
      topics: topicsToCreate.map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1,
      })),
    });

    console.log("🚀 Topics created:", topicsToCreate.join(", "));
  } catch (error) {
    console.error("❌ Error creating topics:", error.message);
  } finally {
    await admin.disconnect();
  }
};

run();
