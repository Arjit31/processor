import { PrismaClient } from "./generated/prisma/client"
import { PrismaPg } from "@prisma/adapter-pg"
import {Kafka} from "kafkajs"
import dotenv from "dotenv";
dotenv.config();

const adapter = new PrismaPg({ 
  connectionString: process.env.DATABASE_URL 
});

const prisma = new PrismaClient({adapter});

const TOPIC_NAME = "zap-events"
const kafka = new Kafka({
  clientId: 'outbox-processor',
  brokers: ['localhost:9092']
})
const producer = kafka.producer()


async function main() {
    await producer.connect()
    while (true) {
        const pendingRows = await prisma.zapRunOutbox.findMany({
            take: 10,
        });
        await producer.send({
            topic: TOPIC_NAME,
            messages: pendingRows.map(r => {
                return {
                    value: JSON.stringify({zapRunId: r.zapRunId, stage: 0, prevMetadata: {}})
                }
            }),
        })
        await prisma.zapRunOutbox.deleteMany({
            where: {
                id: {
                    in: pendingRows.map(x => x.id)
                }
            }
        })
    }
}

main();
