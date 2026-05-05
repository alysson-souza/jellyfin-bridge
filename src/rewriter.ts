import { bridgeItemId, bridgeMediaSourceId } from "./ids.js";

export interface RewriteContext {
  serverId: string;
  bridgeServerId: string;
  itemIdMap?: Map<string, string>;
  mediaSourceIdMap?: Map<string, string>;
  itemId?: string;
  rewriteUnknownItemIds?: boolean;
}

export const ITEM_ID_FIELDS = [
  "Id",
  "ParentId",
  "SeriesId",
  "SeasonId",
  "AlbumId",
  "AlbumArtistId",
  "ChannelId",
  "TopParentId",
  "ParentLogoItemId",
  "ParentArtItemId",
  "ParentBackdropItemId",
  "ParentPrimaryImageItemId",
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
  const mediaSourceIds = new Map(context.mediaSourceIdMap);
  if (Array.isArray(object.MediaSources)) {
    const originalSources = object.MediaSources;
    object.MediaSources = originalSources.map((source) => {
      const rewritten = rewriteMediaSource(source, object.Id, context);
      if (source && typeof source === "object" && rewritten && typeof rewritten === "object") {
        const originalId = (source as Record<string, unknown>).Id;
        const rewrittenId = (rewritten as Record<string, unknown>).Id;
        if (typeof originalId === "string" && typeof rewrittenId === "string") {
          mediaSourceIds.set(originalId, rewrittenId);
        }
      }
      return rewritten;
    });
  }
  if (object.Trickplay && typeof object.Trickplay === "object" && !Array.isArray(object.Trickplay)) {
    object.Trickplay = rewriteTrickplayKeys(object.Trickplay as Record<string, unknown>, mediaSourceIds);
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

function rewriteTrickplayKeys(value: Record<string, unknown>, mediaSourceIds: Map<string, string>): Record<string, unknown> {
  const rewritten: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value)) {
    rewritten[mediaSourceIds.get(key) ?? key] = child;
  }
  return rewritten;
}

function rewriteMediaSource(source: unknown, bridgeItemIdValue: unknown, context: RewriteContext): unknown {
  if (!source || typeof source !== "object") return source;
  const object = { ...(source as Record<string, unknown>) };
  const itemId = typeof bridgeItemIdValue === "string" ? bridgeItemIdValue : context.itemId;
  if (typeof object.Id === "string") {
    object.Id = bridgeMediaSourceId(context.serverId, itemId ?? "", object.Id);
  }
  if (typeof object.ItemId === "string") {
    object.ItemId = rewriteItemId(context, object.ItemId);
  } else if (typeof itemId === "string") {
    object.ItemId = itemId;
  }
  return object;
}

function rewriteItemId(context: RewriteContext, upstreamId: string): string {
  return context.itemIdMap?.get(upstreamId)
    ?? (context.rewriteUnknownItemIds === false ? upstreamId : bridgeItemId(`${context.serverId}:${upstreamId}`));
}
