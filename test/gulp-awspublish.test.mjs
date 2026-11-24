import assert from "node:assert/strict";
import { pipeline } from "node:stream/promises";
import { setTimeout } from "node:timers";
import { test } from "node:test";
import { fileURLToPath, URL } from "node:url";
import awspublish from "gulp-awspublish";
import gulp from "gulp";
import rename from "gulp-rename";
import { createTransformPool } from "object-transform-pool";

const concurrency = 2;
const cwd = fileURLToPath(new URL("../", import.meta.url));
const files = ["README.md", "package.json", "tsconfig.json", "tsdown.config.ts"];

function createUploadDemo(failedKey) {
  const publisher = awspublish.create({
    region: "us-east-1",
    params: { Bucket: "object-transform-pool-demo" },
    credentials: { accessKeyId: "demo", secretAccessKey: "demo" },
  });

  const failure = new Error("Demo S3 upload failed");
  const stats = { active: 0, peak: 0, streams: 0, uploaded: [] };

  // Keep the real publish and reporter streams, but stub the S3 write.
  publisher.client.putObject = ({ Key }, callback) => {
    stats.active++;
    stats.peak = Math.max(stats.peak, stats.active);

    setTimeout(() => {
      stats.active--;
      if (Key === failedKey) {
        callback(failure);
        return;
      }

      stats.uploaded.push(Key);
      callback(null, {});
    }, 10);
  };

  const upload = createTransformPool(
    () => {
      stats.streams++;
      return publisher.publish({}, { putOnly: true });
    },
    { concurrency },
  );

  const output = [];
  const done = pipeline(
    gulp.src(files, { cwd, base: cwd, encoding: false }),
    rename((file) => {
      file.dirname = "demo";
    }),
    upload,
    awspublish.reporter({ states: ["put"] }),
    async (source) => {
      for await (const file of source) output.push(file.s3.path);
    },
  );

  return { done, failure, output, stats };
}

test("uploads every file and reuses a bounded number of publish streams", async () => {
  const { done, output, stats } = createUploadDemo();
  await done;

  const expected = files.map((file) => `demo/${file}`).sort();
  assert.deepEqual(stats.uploaded.sort(), expected);
  assert.deepEqual(output.sort(), expected);
  assert.equal(stats.active, 0);
  assert.equal(stats.peak, concurrency);
  assert.equal(stats.streams, concurrency);
});

test("rejects the entire pipeline when one S3 write fails", async () => {
  const { done, failure } = createUploadDemo("demo/package.json");

  await assert.rejects(done, (error) => error === failure);
});
