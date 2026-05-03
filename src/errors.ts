import type { FastifyReply } from "fastify";

export function unsupported(reply: FastifyReply, message = "Endpoint is not supported by Jellyfin Bridge"): void {
  reply.code(501).send({
    type: "https://jellyfin.org/docs/general/server/api/",
    title: "Not Implemented",
    status: 501,
    detail: message
  });
}

export function notFound(reply: FastifyReply, message = "Resource not found"): void {
  reply.code(404).send({
    type: "https://jellyfin.org/docs/general/server/api/",
    title: "Not Found",
    status: 404,
    detail: message
  });
}

export function badGatewayError(message: string): Error & { statusCode: number } {
  return Object.assign(new Error(message), { statusCode: 502 });
}
