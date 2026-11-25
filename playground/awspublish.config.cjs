/* global module */

module.exports = {
  region: process.env.S3_REGION || "us-east-1",
  bucket: process.env.S3_BUCKET,
  accessKeyId: process.env.S3_ACCESS_KEY_ID,
  secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
  publish: {
    headers: {},
  },
};
