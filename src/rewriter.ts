import { bridgeItemId, bridgeMediaSourceId } from "./ids.js";

export interface RewriteContext {
  serverId: string;
  bridgeServerId: string;
  itemIdMap?: Map<string, string>;
}

const ITEM_ID_FIELDS = [
  "Id",
  "ParentId",
  "SeriesId",
  "SeasonId",
  "AlbumId",
  "AlbumArtistId",
  "ChannelId",
  "TopParentId",
  "ParentLogoItemId",
  "ParentBackdropItemId",
  "ParentThumbItemId",
  "PrimaryImageItemId"
];

export function rewriteDto<T>(value: T, context: RewriteContext): T {
  if (Array.isArray(value)) {
    return value.map((item) => rewriteDto(item, context)) as T;
  }
  if (!value || typeof value !== "object") {
    return value;
  }

  const object = { ...(value as Record<string, unknown>) };
  if ("ServerId" in object) {
    object.ServerId = context.bridgeServerId;
  }
  for (const field of ITEM_ID_FIELDS) {
    if (typeof object[field] === "string") {
      object[field] = rewriteItemId(context, object[field]);
    }
  }
  if (Array.isArray(object.MediaSources)) {
    object.MediaSources = object.MediaSources.map((source) => rewriteMediaSource(source, object.Id, context));
  }
  if (object.UserData && typeof object.UserData === "object" && typeof object.Id === "string") {
    object.UserData = { ...(object.UserData as Record<string, unknown>), Key: object.Id, ItemId: object.Id };
  }

  for (const [key, child] of Object.entries(object)) {
    if (key === "MediaSources" || key === "UserData") continue;
    if (child && typeof child === "object") {
      object[key] = rewriteDto(child, context);
    }
  }
  return object as T;
}

function rewriteMediaSource(source: unknown, bridgeItemIdValue: unknown, context: RewriteContext): unknown {
  if (!source || typeof source !== "object") return source;
  const object = { ...(source as Record<string, unknown>) };
  if (typeof object.Id === "string") {
    object.Id = bridgeMediaSourceId(context.serverId, String(bridgeItemIdValue ?? ""), object.Id);
  }
  if (typeof object.ItemId === "string") {
    object.ItemId = rewriteItemId(context, object.ItemId);
  } else if (typeof bridgeItemIdValue === "string") {
    object.ItemId = bridgeItemIdValue;
  }
  return object;
}

function rewriteItemId(context: RewriteContext, upstreamId: string): string {
  return context.itemIdMap?.get(upstreamId) ?? bridgeItemId(`${context.serverId}:${upstreamId}`);
}
