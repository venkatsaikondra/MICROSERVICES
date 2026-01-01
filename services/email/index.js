import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "email-service",
  brokers: ["localhost:9092"],
});

// ✅ Unique consumer group
const consumer = kafka.consumer({ groupId: "email-service" });
const producer = kafka.producer();

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();

    // ✅ Consume order events (NOT payment)
    await consumer.subscribe({
      topic: "order-successful",
      fromBeginning: false,
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        const value = message.value.toString();
        const { userId, orderId } = JSON.parse(value);

        console.log(
          `Email Service: Email sent to user ${userId} for order ${orderId}`
        );

        const emailId = `EMAIL-${Date.now()}`;

        // ✅ Produce email-successful event
        await producer.send({
          topic: "email-successful",
          messages: [
            {
              key: String(userId),
              value: JSON.stringify({
                userId,
                orderId,
                emailId,
                timestamp: Date.now(),
              }),
            },
          ],
        });
      },
    });
  } catch (err) {
    console.error("Email service error:", err);
  }
};

run();
