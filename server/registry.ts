import type { Connection } from "../shared/config.ts";
import { maxConnections, registryDocumentSchema } from "../shared/config.ts";
import { UserFacingError } from "../shared/errors.ts";
import { atomicWrite, readJson, WriteChain } from "./files.ts";
import { registryPath } from "./paths.ts";

/** Durable connection registry. Secrets never live here; config writes carry an
 * optimistic revision so two clients cannot clobber each other's edits. */
export class Registry {
  private document = registryDocumentSchema.parse({ version: 1, connections: [] });
  private readonly chain = new WriteChain();
  private readonly path: string;

  constructor(path: string = registryPath()) {
    this.path = path;
  }

  async load(): Promise<void> {
    const stored = await readJson(this.path, registryDocumentSchema.parse);
    if (stored) this.document = stored;
  }

  list(): Connection[] {
    return this.document.connections;
  }

  get(id: string): Connection | undefined {
    return this.document.connections.find((connection) => connection.id === id);
  }

  async add(connection: Connection): Promise<void> {
    await this.chain.run(async () => {
      if (this.document.connections.length >= maxConnections) {
        throw new UserFacingError(`最多支持 ${maxConnections} 个连接。`);
      }
      if (this.document.connections.some((entry) => entry.id === connection.id)) {
        throw new UserFacingError("连接已存在。");
      }
      this.document.connections.push(connection);
      await atomicWrite(this.path, this.document);
    });
  }

  async replace(next: Connection): Promise<Connection> {
    return this.chain.run(() => {
      const index = this.document.connections.findIndex((entry) => entry.id === next.id);
      if (index < 0) throw new UserFacingError("连接不存在。");
      const current = this.document.connections[index]!;
      if (current.revision !== next.revision) {
        throw new UserFacingError("设置已在其他客户端修改，请刷新后重试。");
      }
      const updated = { ...next, revision: next.revision + 1 } as Connection;
      this.document.connections[index] = updated;
      return atomicWrite(this.path, this.document).then(() => updated);
    });
  }

  /** Bump revision only when a field the user does not edit changes (status bookkeeping). */
  async mutate(id: string, mutate: (connection: Connection) => Connection): Promise<Connection | undefined> {
    return this.chain.run(async () => {
      const index = this.document.connections.findIndex((entry) => entry.id === id);
      if (index < 0) return undefined;
      const updated = mutate(this.document.connections[index]!);
      this.document.connections[index] = updated;
      await atomicWrite(this.path, this.document);
      return updated;
    });
  }

  async remove(id: string): Promise<boolean> {
    return this.chain.run(async () => {
      const before = this.document.connections.length;
      this.document.connections = this.document.connections.filter((entry) => entry.id !== id);
      const removed = this.document.connections.length < before;
      if (removed) await atomicWrite(this.path, this.document);
      return removed;
    });
  }
}
