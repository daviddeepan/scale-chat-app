import { PrismaClient } from "@prisma/client";
import exp from "constants";

const prismaClient = new PrismaClient({
	log: ["query"],
});

export default prismaClient
