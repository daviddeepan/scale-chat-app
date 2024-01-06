import { Server } from "socket.io";
import Redis from "ioredis";
import prismaClient from "./prisma";
import { produceMessage } from "./kafka";

const pub = new Redis({
	host: "redis-1fedba27-david-3f99.a.aivencloud.com",
	port: 10106,
	username: "default",
	password: "AVNS_63wUQpNl3nrhW7Br16D",
});
const sub = new Redis({
	host: "redis-1fedba27-david-3f99.a.aivencloud.com",
	port: 10106,
	username: "default",
	password: "AVNS_63wUQpNl3nrhW7Br16D",
});

class SocketService {
	private _io: Server;
	constructor() {
		console.log("Init Socket Service....");
		this._io = new Server({
			cors: {
				allowedHeaders: ["*"],
				origin: "*",
			},
		});
		sub.subscribe("MESSAGES");
	}

	public initListeners() {
		const io = this._io;
		console.log("Init Socket Listeners....");
		io.on("connect", (socket) => {
			console.log(`New socket Connected`, socket.id);
			socket.on(
				"event:message",
				async ({ message }: { message: string }) => {
					console.log("New Message Recieved ->", message);
					await pub.publish("MESSAGES", JSON.stringify({ message }));
				}
			);
		});
		sub.on("message", async (channel, message) => {
			if (channel === "MESSAGES") {
				io.emit("message", message);
				await produceMessage(message);
				console.log("message produces to kafka broker....");
			}
		});
	}

	get io() {
		return this._io;
	}
}

export default SocketService;
