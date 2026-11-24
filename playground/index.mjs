import { randomUUID } from "node:crypto";
import { Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { fileURLToPath, URL } from "node:url";
import awspublish from "gulp-awspublish";
import gulp from "gulp";
import rename from "gulp-rename";
import { createTransformPool } from "object-transform-pool";

const bucket = process.env.S3_BUCKET?.trim();
const region = process.env.AWS_REGION?.trim();

if (!bucket || !region) {
  throw new Error("Set S3_BUCKET and AWS_REGION before running the playground.");
}

const cwd = fileURLToPath(new URL("./fixtures/", import.meta.url));
const prefix = `object-transform-pool-demo/${randomUUID()}`;

// The AWS SDK resolves credentials from its default provider chain.
const publisher = awspublish.create({
  region,
  params: { Bucket: bucket },
});

await pipeline(
  gulp.src("*.txt", { cwd, base: cwd, encoding: false }),
  rename((file) => {
    file.dirname = prefix;
  }),
  createTransformPool(() => publisher.publish({}, { putOnly: true }), {
    concurrency: 2,
  }),
  awspublish.reporter({ states: ["put"] }),
  new Writable({
    objectMode: true,
    write(_file, _encoding, callback) {
      callback();
    },
  }),
);

console.log(`Upload complete: s3://${bucket}/${prefix}/`);
