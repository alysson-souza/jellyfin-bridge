export interface ClosableApp {
  close(): Promise<unknown>;
}

export interface ClosableStore {
  close(): void;
}

export interface Stoppable {
  stop(): void;
}

export interface SignalProcess {
  on(event: "SIGINT" | "SIGTERM", listener: () => void): unknown;
  exit(code?: number): never;
}

export function installShutdownHandlers(
  processLike: SignalProcess,
  app: ClosableApp,
  store: ClosableStore,
  scheduler?: Stoppable
): void {
  let shuttingDown = false;
  const shutdown = async (): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    try {
      scheduler?.stop();
      await app.close();
      store.close();
      processLike.exit(0);
    } catch {
      processLike.exit(1);
    }
  };

  processLike.on("SIGINT", () => {
    void shutdown();
  });
  processLike.on("SIGTERM", () => {
    void shutdown();
  });
}
