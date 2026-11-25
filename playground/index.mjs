import { readFile } from "node:fs/promises";
import { Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { fileURLToPath, URL } from "node:url";
import awspublish from "gulp-awspublish";
import gulp from "gulp";
import rename from "gulp-rename";
import minimist from "minimist";
import { createTransformPool } from "object-transform-pool";
import s3Config from "./awspublish.config.cjs";

const defaultReporterStates = ["create", "update", "put", "skip", "cache", "delete"];

const argv = process.argv.slice(2);
const separator = argv.indexOf("--");
const flags = separator === -1 ? argv : argv.slice(0, separator);
const args = minimist(argv, {
  boolean: ["simulate"],
  default: { simulate: true },
  string: ["region", "bucket", "access-key-id", "secret-access-key"],
  alias: {
    "access-key-id": "accessKeyId",
    "secret-access-key": "secretAccessKey",
  },
});
const config = {
  ...s3Config,
  region: args.region ?? s3Config.region,
  bucket: (args.bucket ?? s3Config.bucket)?.trim(),
  accessKeyId: args["access-key-id"] ?? s3Config.accessKeyId,
  secretAccessKey: args["secret-access-key"] ?? s3Config.secretAccessKey,
};
await upload(config, args.simulate || !flags.includes("--no-simulate"));

async function upload(config, simulate) {
  if (!config.region) {
    throw new Error("S3 region is required.");
  }
  if (!config.bucket) {
    throw new Error("Specify an S3 bucket with --bucket or S3_BUCKET.");
  }

  const versionFile = new URL("./version.json", import.meta.url);
  const version = JSON.parse(await readFile(versionFile, "utf8"))?.version;
  if (typeof version !== "string" || !version.trim()) {
    throw new Error(`Version file does not contain a valid version: ${versionFile.pathname}`);
  }

  const credentials =
    config.accessKeyId && config.secretAccessKey
      ? { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey }
      : undefined;
  const publisher = awspublish.create({
    region: config.region,
    params: { Bucket: config.bucket },
    credentials,
  });
  const headers = {
    "Cache-Control": "max-age=315360000, public",
    "x-amz-acl": "public-read",
    ...config.publish?.headers,
  };
  const options = { force: true, ...config.publish?.options, simulate };
  const cwd = fileURLToPath(new URL("./fixtures/", import.meta.url));
  const prefix = `${config.keyPrefix ?? "nuxt/pc"}/${version}`;
  const reporters =
    config.reporter === false
      ? []
      : [
          awspublish.reporter({
            states: config.reporter?.states ?? defaultReporterStates,
          }),
        ];

  await pipeline(
    gulp.src("**/*", { cwd, base: cwd, encoding: false }),
    rename((file) => {
      file.dirname = `${prefix}/${file.dirname}`;
    }),
    createTransformPool(() => publisher.publish(headers, options), {
      concurrency: config.concurrency ?? 2,
    }),
    ...reporters,
    new Writable({
      objectMode: true,
      write(_file, _encoding, callback) {
        callback();
      },
    }),
  );

  const status = simulate ? "Simulation complete" : "Upload complete";
  console.log(`${status}: s3://${config.bucket}/${prefix}/`);
}
