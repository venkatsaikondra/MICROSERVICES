import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "analytic-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "analytic-service",

  // ✅ STATIC MEMBERSHIP (stops rebalancing storms)
  groupInstanceId: "analytic-instance-1",

  // ✅ Stability tuning
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

const run = async () => {
  try {
    await consumer.connect();

    // ✅ Do NOT replay old messages
    await consumer.subscribe({ topic: "payment-successful", fromBeginning: false });
    await consumer.subscribe({ topic: "order-successful", fromBeginning: false });
    await consumer.subscribe({ topic: "email-successful", fromBeginning: false });

    await consumer.run({
      autoCommit: true,

      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());

        switch (topic) {
          case "payment-successful": {
            const { userId, cart } = data;
            const total = cart
              .reduce((acc, item) => acc + item.price, 0)
              .toFixed(2);

            console.log(`📊 Analytics: User ${userId} paid ₹${total}`);
            break;
          }

          case "order-successful": {
            const { userId, orderId } = data;
            console.log(`📦 Analytics: Order ${orderId} for user ${userId}`);
            break;
          }

          case "email-successful": {
            const { userId, emailId } = data;
            console.log(`📧 Analytics: Email ${emailId} sent to user ${userId}`);
            break;
          }
        }
      },
    });
  } catch (err) {
    console.error("Analytics error:", err);
  }
};

run();
