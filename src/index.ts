import { PrismaClient } from "./generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Kafka } from "kafkajs";
import * as fs from 'fs'; 
import path from "path";
import dotenv from "dotenv";
import express from "express";
import cors from "cors";

dotenv.config();
const app = express();
const corsOptions = {
    credentials: true,
    origin: process.env.FRONTEND_URL,
};
app.use(cors(corsOptions));
const PORT = process.env.PORT || 3004;

app.get("/", (req, res) => {
  console.log("pinged")
  res.status(200).send("OK");
});

app.listen(PORT, () => {
  console.log(`Ping server running on ${PORT}`);
});

const adapter = new PrismaPg({
    connectionString: process.env.DATABASE_URL,
});

const prisma = new PrismaClient({ adapter });

const TOPIC_NAME = "zap-events";
// const kafka = new Kafka({
//     clientId: "outbox-processor",
//     brokers: ["localhost:9092"],
// });
const kafka = new Kafka({
  clientId: "outbox-processor",
  brokers: ["kafka-378cf09f-arjit-chat-db.l.aivencloud.com:20666"],
  ssl: {
    key: fs.readFileSync(
      path.join(__dirname, "../certs/service.key"),
      "utf-8"
    ),
    cert: fs.readFileSync(
      path.join(__dirname, "../certs/service.cert"),
      "utf-8"
    ),
    ca: [
      fs.readFileSync(
        path.join(__dirname, "../certs/ca.pem"),
        "utf-8"
      ),
    ],
  },
});
const producer = kafka.producer();

async function main() {
    await producer.connect();
    while (true) {
        try {
            const pendingRows = await prisma.zapRunOutbox.findMany({
                take: 10,
            });
            await producer.send({
                topic: TOPIC_NAME,
                messages: pendingRows.map((r) => {
                    return {
                        value: JSON.stringify({
                            zapRunId: r.zapRunId,
                            stage: 0,
                            prevMetadata: {},
                        }),
                    };
                }),
            });
            await prisma.zapRunOutbox.deleteMany({
                where: {
                    id: {
                        in: pendingRows.map((x) => x.id),
                    },
                },
            });
        } catch (error) {
            console.log(error);
        }
    }
}

main();