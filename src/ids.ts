import { createHash, randomUUID } from "node:crypto";

const BRIDGE_NAMESPACE = "8e58f9d0-1c1b-4ef5-a1df-86493dbf4b2b";

export function newToken(): string {
  return randomUUID().replaceAll("-", "") + randomUUID().replaceAll("-", "");
}

export function bridgeServerId(configName: string): string {
  return uuidV5(`server:${configName}`, BRIDGE_NAMESPACE).replaceAll("-", "");
}

export function bridgeLibraryId(libraryId: string): string {
  return uuidV5(`library:${libraryId}`, BRIDGE_NAMESPACE).replaceAll("-", "");
}

export function passThroughLibraryId(server: string, libraryId: string): string {
  return uuidV5(`library:${server}:${libraryId}`, BRIDGE_NAMESPACE).replaceAll("-", "");
}

export function bridgeItemId(logicalKey: string): string {
  return uuidV5(`item:${logicalKey}`, BRIDGE_NAMESPACE).replaceAll("-", "");
}

export function bridgeMediaSourceId(server: string, itemId: string, mediaSourceId: string): string {
  return uuidV5(`media-source:${server}:${itemId}:${mediaSourceId}`, BRIDGE_NAMESPACE).replaceAll("-", "");
}

export function uuidV5(name: string, namespace: string): string {
  const namespaceBytes = uuidToBytes(namespace);
  const hash = createHash("sha1").update(namespaceBytes).update(name).digest();
  const bytes = Uint8Array.from(hash.subarray(0, 16));
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  return bytesToUuid(bytes);
}

function uuidToBytes(uuid: string): Uint8Array {
  const hex = uuid.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) {
    throw new Error(`Invalid UUID namespace ${uuid}`);
  }
  return Uint8Array.from(hex.match(/../g)!.map((byte) => Number.parseInt(byte, 16)));
}

function bytesToUuid(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
