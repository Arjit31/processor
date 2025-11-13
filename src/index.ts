import { PrismaClient } from "@prisma/client";
import {Kafka} from "kafkajs"

const prisma = new PrismaClient();

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
                    value: r.zapRunId
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
