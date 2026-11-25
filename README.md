# object-transform-pool

Run Node.js object-mode transforms concurrently with a pool of reusable instances.

Each worker handles one input at a time. The pool creates workers on demand,
reuses them across inputs, and releases them when the pipeline ends.

## Installation

Requires Node.js 22 or later.

```sh
npm install object-transform-pool
# or
pnpm add object-transform-pool
```

## Quick start

Pass a factory that creates a fresh object-mode `Transform`, then use the pool
as a stage in Node.js `pipeline()`.

```js
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { setTimeout } from "node:timers";
import { createTransformPool } from "object-transform-pool";

const pool = createTransformPool(
  () =>
    new Transform({
      objectMode: true,
      transform(item, _encoding, callback) {
        setTimeout(() => {
          callback(null, { name: item.name.toUpperCase() });
        }, item.delay);
      },
    }),
  { concurrency: 2 },
);

await pipeline(
  Readable.from([
    { name: "first", delay: 20 },
    { name: "second", delay: 5 },
  ]),
  pool,
  async (output) => {
    for await (const item of output) console.log(item.name);
  },
);
```

## API

### `createTransformPool(createTransform, options?)`

Returns a Node.js object-mode `Transform`.

| Parameter | Description |
| --- | --- |
| `createTransform` | Factory that returns a fresh object-mode `Transform` each time it is called. |
| `options.concurrency` | Maximum number of writes in flight. Defaults to `10` and must be a positive integer. |

### Worker requirements

- A worker can emit **zero or one object per input**. Emit the object before the
  write callback completes, or complete the callback without an output to skip
  the input. A second output for the same input fails the pipeline.
- Keep **`emitClose: true`**, the Node.js default. The pool uses the worker's
  `close` event to detect a write interrupted by an early close.
- Use workers that process each input independently and produce all output
  during writes. Workers are destroyed when the pool closes, so output that
  depends on `flush` is unsupported.

### Completion and errors

Outputs arrive in completion order and may differ from input order.
`await pipeline(...)` resolves after all inputs finish. Worker errors reject
the pipeline. The pool destroys its workers after completion, failure, or
cancellation. Side effects from work already completed remain in place.

## S3 playground

The [playground](https://github.com/yisibell/object-transform-pool/tree/main/playground)
uses Gulp and `gulp-awspublish` to process the files in `playground/fixtures`.
It reads settings from `playground/awspublish.config.cjs` and the version from
`playground/version.json`. The default key layout is
`nuxt/pc/<version>/<relative-path>`. Set `keyPrefix` or change the version to
use a different destination prefix.

### Simulate an upload

Simulation is the default. It makes no S3 requests, so a placeholder bucket
name is enough and AWS credentials are unnecessary.

```sh
pnpm playground --region us-east-1 --bucket demo-bucket
```

### Upload to S3

Pass `--no-simulate` and explicitly provide an existing bucket via `--bucket`
or `S3_BUCKET`. Use credentials with permission to write to that bucket.

```sh
pnpm playground --no-simulate --region us-east-1 --bucket my-existing-bucket
```

The playground follows the `nuxt-conf` upload defaults for caching and forced
uploads. Reusing the same prefix and version can overwrite existing objects.
For buckets that disable ACLs, set `"x-amz-acl": undefined` in
`publish.headers` to omit the default `public-read` ACL.

### Configuration

CLI flags override values from the playground configuration.

| Flag | Environment fallback | Purpose |
| --- | --- | --- |
| `--region` | `S3_REGION` | S3 region. Defaults to `us-east-1`. |
| `--bucket` | `S3_BUCKET` | Destination bucket. |
| `--access-key-id` | `S3_ACCESS_KEY_ID` | Explicit AWS access key ID. |
| `--secret-access-key` | `S3_SECRET_ACCESS_KEY` | Explicit AWS secret access key. |
| `--no-simulate` | — | Enables real uploads. |

The credential flags also accept `--accessKeyId` and `--secretAccessKey`.
When explicit credentials are absent, the AWS SDK uses its default credential chain.

## Development

```sh
pnpm install
pnpm test
pnpm typecheck
pnpm lint
```

The test suite uses simulated S3 requests and runs without AWS credentials.

## License

[MIT](LICENSE)
