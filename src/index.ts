import type { Transform } from "node:stream";
import { transform } from "stream-transform";

export interface TransformPoolOptions {
  /** Maximum number of in-flight writes. Defaults to 10. */
  concurrency?: number;
}

/**
 * Creates an object-mode Transform that processes inputs concurrently using reusable workers.
 *
 * Each worker handles one write at a time. It may emit zero or one object per
 * write and must emit it before the write callback completes. A second output
 * or a worker error fails the pipeline. Results are emitted in completion order.
 *
 * @param createTransform - Factory that returns a fresh object-mode Transform.
 *   Workers must keep `emitClose` enabled and produce all output during writes.
 * @param options - Pool settings. The default concurrency is 10.
 * @returns An object-mode Transform that owns the workers and destroys them on close.
 * @throws {RangeError} If concurrency is not a positive integer.
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
    // A worker returns to the idle pool only after its current write completes.
    let worker = idle.pop();
    if (!worker) {
      worker = createTransform();
      workers.add(worker);
      // Handle stream errors even when they arrive outside the write callback.
      worker.on("error", (error: Error) => {
        pool.destroy(error);
      });
    }

    const stream = worker;
    let output: object | null = null;
    let onClose: (() => void) | undefined;
    // Hold the output until the write succeeds so partial results stay out of the pipeline.
    const onData = (chunk: object) => {
      if (output !== null) {
        pool.destroy(new Error("Transform emitted multiple objects for one input"));
        return;
      }

      output = chunk;
    };

    try {
      await new Promise<void>((resolve, reject) => {
        // An early close can leave the write callback unsettled.
        onClose = () => reject(new Error("Transform closed before its write completed"));
        stream.once("close", onClose);
        stream.on("data", onData);
        stream.write(input, (error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    } finally {
      // Remove per-write listeners before the worker can handle another input.
      stream.off("data", onData);
      if (onClose) stream.off("close", onClose);
    }

    if (!worker.destroyed && !pool.destroyed) idle.push(worker);
    return output;
  });

  // Include busy workers when the pipeline fails or is cancelled.
  pool.once("close", () => {
    for (const worker of workers) worker.destroy();
    workers.clear();
    idle.length = 0;
  });

  return pool;
}
