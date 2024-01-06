import { Kafka, Producer } from "kafkajs";
import fs from "fs";
import path from "path";
import prismaClient from "./prisma";

const kafka = new Kafka({
	brokers: ["kafka-82293ea-david-3f99.a.aivencloud.com:10119"],
	ssl: {
		ca: [fs.readFileSync(path.resolve("./ca (1).pem"), "utf-8")],
	},
	sasl: {
		username: "avnadmin",
		password: "AVNS_tehHuzBbYcktYUrHLIx",
		mechanism: "plain",
	},
});

let producer: null | Producer = null;

export async function createProducer() {
	if (producer) return producer;

	const _producer = kafka.producer();
	await _producer.connect();
	producer = _producer;
	return producer;
}

export async function produceMessage(message: string) {
	const producer = await createProducer();
	producer.send({
		messages: [{ key: `message - ${Date.now()}`, value: message }],
		topic: "MESSAGES",
	});
	return true;
}

export async function startConsumer() {
	console.log('consumer started')
	const consumer = kafka.consumer({ groupId: "default" });
	await consumer.connect();
	await consumer.subscribe({ topic: "MESSAGES" });

	await consumer.run({
		autoCommit: true,
		eachMessage: async ({ message, pause }) => {
			if (!message.value) return;
			console.log(`New Message Recv ....`);
			try {
				await prismaClient.message.create({
					data: {
						text: message.value?.toString(),
					},
				});
			} catch (error) {
				console.log("Error ->->-<");
				pause();
				setTimeout(() => {
					consumer.resume([{ topic: "MESSAGES" }]);
				}, 60 * 1000);
			}
		},
	});
}
export default kafka;
