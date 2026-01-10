"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const client_1 = require("./generated/prisma/client");
const adapter_pg_1 = require("@prisma/adapter-pg");
const kafkajs_1 = require("kafkajs");
const fs = __importStar(require("fs"));
const path_1 = __importDefault(require("path"));
const dotenv_1 = __importDefault(require("dotenv"));
const express_1 = __importDefault(require("express"));
const cors_1 = __importDefault(require("cors"));
dotenv_1.default.config();
const app = (0, express_1.default)();
const corsOptions = {
    credentials: true,
    origin: process.env.FRONTEND_URL,
};
app.use((0, cors_1.default)(corsOptions));
const PORT = process.env.PORT || 3004;
app.get("/", (req, res) => {
    console.log("pinged");
    res.status(200).send("OK");
});
app.listen(PORT, () => {
    console.log(`Ping server running on ${PORT}`);
});
const adapter = new adapter_pg_1.PrismaPg({
    connectionString: process.env.DATABASE_URL,
});
const prisma = new client_1.PrismaClient({ adapter });
const TOPIC_NAME = "zap-events";
// const kafka = new Kafka({
//     clientId: "outbox-processor",
//     brokers: ["localhost:9092"],
// });
const kafka = new kafkajs_1.Kafka({
    clientId: "outbox-processor",
    brokers: ["kafka-378cf09f-arjit-chat-db.l.aivencloud.com:20666"],
    ssl: {
        key: fs.readFileSync(process.env.ENVIRONMET === "DEV"
            ? path_1.default.join(__dirname, "../certs/service.key")
            : path_1.default.join(__dirname, "../../certs/service.key"), "utf-8"),
        cert: fs.readFileSync(process.env.ENVIRONMET === "DEV"
            ? path_1.default.join(__dirname, "../certs/service.cert")
            : path_1.default.join(__dirname, "../../certs/service.cert"), "utf-8"),
        ca: [
            fs.readFileSync(process.env.ENVIRONMET === "DEV"
                ? path_1.default.join(__dirname, "../certs/ca.pem")
                : path_1.default.join(__dirname, "../../certs/ca.pem"), "utf-8"),
        ],
    },
});
const producer = kafka.producer();
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        yield producer.connect();
        while (true) {
            try {
                const pendingRows = yield prisma.zapRunOutbox.findMany({
                    take: 10,
                });
                // add throttling
                if (pendingRows.length === 0) {
                    new Promise((resolve) => {
                        setTimeout(resolve, 2000);
                    }).then();
                    continue;
                }
                yield producer.send({
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
                yield prisma.zapRunOutbox.deleteMany({
                    where: {
                        id: {
                            in: pendingRows.map((x) => x.id),
                        },
                    },
                });
            }
            catch (error) {
                console.log(error);
                new Promise((resolve) => {
                    setTimeout(resolve, 2000);
                }).then();
            }
        }
    });
}
main();
