# object-transform-pool
Run Node.js object-mode transforms concurrently with a pool of reusable instances.


## Install

Requires Node.js 22 or later.

```sh
pnpm add object-transform-pool
```

## Usage

Pass a factory that creates a fresh Node.js object-mode Transform. The pool
creates workers as needed and reuses them across inputs. Use `pipeline()` to
wait for completion and propagate errors.

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
          callback(null, { ...item, name: item.name.toUpperCase() });
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

`createTransformPool(createTransform, options?)` returns an object-mode
`Transform`.

- **`createTransform`** creates one fresh worker each time it is called. Each
  worker processes one input at a time and can be reused for later inputs.
- **`options.concurrency`** limits the number of active workers. It defaults to
  `10` and must be a positive integer.
- **Worker output** is limited to zero or one object per input. A worker must
  emit its output before its write completes. Use workers that process inputs
  independently and deliver all output during their writes.
- **Completion** waits for all inputs to finish. Results arrive in completion
  order and may differ from input order. A worker error fails the pipeline.
  Work that has already completed keeps its side effects.

## S3 playground

Use the pool as the publish stage in a Gulp pipeline.

```js
const publish = createTransformPool(
  () => publisher.publish(headers, { putOnly: true }),
  { concurrency: 5 },
);
```

See [the full playground](playground/index.mjs) for the source stream, S3 key
prefix, reporter, and pipeline wiring.

The playground uploads the two sample files in `playground/fixtures` to a
unique prefix in an existing S3 bucket. Use AWS credentials with
`s3:PutObject` permission for that bucket. The AWS SDK uses its default
credential chain, so you can use an AWS profile or credentials already
configured in your environment.

```powershell
$env:AWS_PROFILE = "my-profile" # Optional if credentials are already configured
$env:AWS_REGION = "us-east-1"
$env:S3_BUCKET = "my-existing-bucket"
pnpm playground
```

The demo prints the S3 prefix after both uploads complete and leaves the files
in the bucket. Run `pnpm test` to check concurrency and upload failure handling
with simulated S3 requests.
