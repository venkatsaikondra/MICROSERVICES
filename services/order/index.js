import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9092"],
});

// ✅ Separate group for order service
const consumer = kafka.consumer({ groupId: "order-service" });
const producer = kafka.producer();

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();

    // Consume payment events
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: false,
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        const value = message.value.toString();
        const { userId, cart } = JSON.parse(value);

        // Simulate order creation
        const orderId = `ORD-${Date.now()}`;

        console.log(
          `Order Service: Order ${orderId} created for user ${userId}`
        );

        // Produce order-successful event
        await producer.send({
          topic: "order-successful",
          messages: [
            {
              key: String(userId),
              value: JSON.stringify({
                userId,
                orderId,
                cart,
                timestamp: Date.now(),
              }),
            },
          ],
        });
      },
    });
  } catch (err) {
    console.error("Order service error:", err);
  }
};

run();
