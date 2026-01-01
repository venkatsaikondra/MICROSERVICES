import express from "express";
import cors from "cors";

const app = express();

// Middleware
app.use(cors({
  origin: "http://localhost:3000"
}));
app.use(express.json());

// Routes
app.post("/payment-service", async (req, res, next) => {
  try {
    const { cart } = req.body;
    const userID = "123";
    // Payment logic here
    // Kafka event here
    res.status(200).json({ message: "Payment processed", userID, cart });
  } catch (err) {
    next(err);
  }
});

// Error handler middleware
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({
    message: "Something went wrong",
    error: err.message
  });
});

// Server start
app.listen(8000, () => {
  console.log("Payment Service is running on port 8000");
});
