/* global AbortController */

import assert from "node:assert/strict";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { test } from "node:test";
import { createTransformPool } from "object-transform-pool";

test("rejects the pipeline when a worker emits multiple objects for one input", async () => {
  const pool = createTransformPool(
    () =>
      new Transform({
        objectMode: true,
        transform(input, _encoding, callback) {
          this.push(input);
          callback(null, { duplicate: input });
        },
      }),
    { concurrency: 2 },
  );
  const received = [];

  await assert.rejects(
    pipeline(Readable.from([{ id: 1 }]), pool, async (output) => {
      for await (const item of output) received.push(item);
    }),
    { message: "Transform emitted multiple objects for one input" },
  );
  assert.deepEqual(received, []);
});

test(
  "rejects the pipeline when a worker closes before its write callback",
  { timeout: 2000 },
  async () => {
    const pool = createTransformPool(
      () =>
        new Transform({
          objectMode: true,
          emitClose: true,
          transform() {
            // Destroy the worker without calling the transform callback.
            this.destroy();
          },
        }),
    );

    await assert.rejects(
      pipeline(Readable.from([{ id: 1 }]), pool, async (output) => {
        for await (const item of output) assert.fail(`Unexpected output: ${JSON.stringify(item)}`);
      }),
      { message: "Transform closed before its write completed" },
    );
  },
);

test("destroys busy and idle workers after a worker fails", { timeout: 2000 }, async () => {
  const failure = new Error("Worker failed");
  const workers = new Map();
  let failWrite;

  const pool = createTransformPool(
    () =>
      new Transform({
        objectMode: true,
        transform(input, _encoding, callback) {
          workers.set(input.role, this);
          if (input.role === "fail") failWrite = callback;
          if (input.role === "idle") callback(null, input);
          // Keep the busy worker's write pending until teardown.
        },
      }),
    { concurrency: 3 },
  );
  const inputs = [{ role: "fail" }, { role: "busy" }, { role: "idle" }];

  await assert.rejects(
    pipeline(Readable.from(inputs), pool, async (output) => {
      for await (const item of output) {
        assert.equal(item.role, "idle");
        assert.equal(workers.size, 3);
        assert.equal(workers.get("busy").writableLength, 1);
        assert.equal(workers.get("idle").writableLength, 0);
        assert.equal(workers.get("busy").destroyed, false);
        assert.equal(workers.get("idle").destroyed, false);
        failWrite(failure);
      }
    }),
    (error) => error === failure,
  );

  assert.equal(workers.get("busy").destroyed, true);
  assert.equal(workers.get("idle").destroyed, true);
});

test("destroys all workers after the pipeline completes", { timeout: 2000 }, async () => {
  const workers = [];
  const pending = [];
  const concurrency = 2;
  const pool = createTransformPool(
    () => {
      const worker = new Transform({
        objectMode: true,
        transform(input, _encoding, callback) {
          // Hold the first write until both workers have started.
          pending.push(() => callback(null, input));
          if (pending.length === concurrency) {
            for (const complete of pending.splice(0)) complete();
          }
        },
      });
      workers.push(worker);
      return worker;
    },
    { concurrency },
  );
  const received = [];

  await pipeline(Readable.from([{ id: 1 }, { id: 2 }]), pool, async (output) => {
    for await (const { id } of output) received.push(id);
  });

  assert.deepEqual(
    received.sort((a, b) => a - b),
    [1, 2],
  );
  assert.equal(workers.length, concurrency);
  for (const worker of workers) assert.equal(worker.destroyed, true);
});

test("destroys all workers when the pipeline is aborted", { timeout: 2000 }, async () => {
  const controller = new AbortController();
  const workers = [];
  const concurrency = 2;
  const allStarted = Promise.withResolvers();
  let started = 0;
  const pool = createTransformPool(
    () => {
      const worker = new Transform({
        objectMode: true,
        transform() {
          // Keep each write pending until the caller aborts the pipeline.
          if (++started === concurrency) allStarted.resolve();
        },
      });
      workers.push(worker);
      return worker;
    },
    { concurrency },
  );

  const completion = pipeline(
    Readable.from([{ id: 1 }, { id: 2 }]),
    pool,
    async (output) => {
      for await (const item of output) assert.fail(`Unexpected output: ${JSON.stringify(item)}`);
    },
    { signal: controller.signal },
  );

  const abortWhenReady = allStarted.promise.then(() => {
    assert.equal(workers.length, concurrency);
    for (const worker of workers) {
      assert.equal(worker.writableLength, 1);
      assert.equal(worker.destroyed, false);
    }
    controller.abort();
  });
  await Promise.all([
    assert.rejects(completion, { name: "AbortError", code: "ABORT_ERR" }),
    abortWhenReady,
  ]);

  for (const worker of workers) assert.equal(worker.destroyed, true);
});

test("reuses the same worker after it skips an input", { timeout: 2000 }, async () => {
  let workersCreated = 0;
  const processed = [];
  const received = [];
  const pool = createTransformPool(
    () => {
      workersCreated++;
      return new Transform({
        objectMode: true,
        transform(input, _encoding, callback) {
          processed.push(input.id);
          if (input.skip) callback();
          else callback(null, input);
        },
      });
    },
    { concurrency: 1 },
  );

  await pipeline(Readable.from([{ id: 1, skip: true }, { id: 2 }]), pool, async (output) => {
    for await (const item of output) received.push(item);
  });

  assert.equal(workersCreated, 1);
  assert.deepEqual(processed, [1, 2]);
  assert.deepEqual(received, [{ id: 2 }]);
});

test("rejects the pipeline when a worker emits an error directly", { timeout: 2000 }, async () => {
  const failure = new Error("Worker emitted an error");
  const pool = createTransformPool(
    () =>
      new Transform({
        objectMode: true,
        transform() {
          // Deliberately leave the write callback pending.
          this.emit("error", failure);
        },
      }),
  );

  await assert.rejects(
    pipeline(Readable.from([{ id: 1 }]), pool, async (output) => {
      for await (const item of output) assert.fail(`Unexpected output: ${JSON.stringify(item)}`);
    }),
    (error) => error === failure,
  );
});
