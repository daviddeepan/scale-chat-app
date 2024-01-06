import http from "http";
import SocketService from "./services/socket";
import { startConsumer } from "./services/kafka";

async function init() {
	startConsumer()
	const httpServer = http.createServer();
	const PORT = process.env.PORT ? process.env.PORT : 8000;

	const socketService = new SocketService();
	socketService.io.attach(httpServer);

	httpServer.listen(PORT, () =>
		console.log(`HTTP Server started at port:${PORT}`)
	);
	socketService.initListeners();
}
init();
