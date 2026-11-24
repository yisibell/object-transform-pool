import type { Transform } from "node:stream";
import { transform } from "stream-transform";

export interface TransformPoolOptions {
  concurrency?: number;
}

/**
 * Run object-mode transforms concurrently, with one write in flight per worker.
 * Each write may emit one object before its callback, or emit nothing.
 * Outputs are emitted in completion order. Workers are destroyed when the pool closes.
 */
export function createTransformPool(
  createTransform: () => Transform,
  { concurrency = 10 }: TransformPoolOptions = {},
): Transform {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new RangeError("concurrency must be a positive integer");
  }

  const idle: Transform[] = [];
  const workers = new Set<Transform>();

  const pool = transform({ parallel: concurrency }, async (input: object) => {
    let worker = idle.pop();
    if (!worker) {
      worker = createTransform();
      workers.add(worker);
      worker.on("error", (error: Error) => {
        pool.destroy(error);
      });
    }

    const stream = worker;
    let output: object | null = null;
    let onClose: (() => void) | undefined;
    const onData = (chunk: object) => {
      output = chunk;
    };

    try {
      await new Promise<void>((resolve, reject) => {
        onClose = () => reject(new Error("Transform closed before its write completed"));
        stream.once("close", onClose);
        stream.on("data", onData);
        stream.write(input, (error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    } finally {
      stream.off("data", onData);
      if (onClose) stream.off("close", onClose);
    }

    if (!worker.destroyed && !pool.destroyed) idle.push(worker);
    return output;
  });

  pool.once("close", () => {
    for (const worker of workers) worker.destroy();
    workers.clear();
    idle.length = 0;
  });

  return pool;
}
