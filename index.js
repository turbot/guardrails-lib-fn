const _ = require("lodash");
const { Turbot } = require("@turbot/sdk");
const errors = require("@turbot/errors");
const fs = require("fs-extra");
const https = require("https");
const log = require("@turbot/log");
const os = require("os");
const path = require("path");
const got = require("got");
const rimraf = require("rimraf");
const streamBuffers = require("stream-buffers");
const taws = require("@turbot/guardrails-aws-sdk-v3");
const tmp = require("tmp");
const url = require("url");
const util = require("util");
const MessageValidator = require("@turbot/sns-validator");
const validator = new MessageValidator();
const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns");
const { KMSClient, DecryptCommand } = require("@aws-sdk/client-kms");

const cachedCredentials = new Map();

const cachedRegions = new Map();
const credentialEnvMapping = new Map([
  ["accesskey", "AWS_ACCESS_KEY"],
  ["accesskeyid", "AWS_ACCESS_KEY_ID"],
  ["secretkey", "AWS_SECRET_KEY"],
  ["secretaccesskey", "AWS_SECRET_ACCESS_KEY"],
  ["sessiontoken", "AWS_SESSION_TOKEN"],
  ["securitytoken", "AWS_SECURITY_TOKEN"],
]);
const regionEnvMapping = new Map([
  ["awsRegion", "AWS_REGION"],
  ["awsDefaultRegion", "AWS_DEFAULT_REGION"],
]);

// used by container, no issue with concurrency / global variable because each container run is in a separate container
let _containerSnsParam;

// Store the credentials and region we receive in the SNS message in the AWS environment variables
const setAWSEnvVars = ($) => {
  // We assume that the AWS credentials from graphql are available in the
  // standard locations (best we can do without a lot of complexity). We
  // go from most rare find to least rare, which is most likely what the
  // developer will expect.
  let credentials = _.get($, ["organization", "credentials"]);
  if (!credentials) {
    credentials = _.get($, ["organizationalUnit", "credentials"]);
    if (!credentials) {
      credentials = _.get($, ["account", "credentials"]);
    }
  }

  if (credentials) {
    // AWS generates accessKeyId (with lower case a) for IAM Role but AccessKeyId
    // (with upper case A) for access key pair.
    credentials = Object.keys(credentials).reduce((c, k) => ((c[k.toLowerCase()] = credentials[k]), c), {});

    for (const [key, envVar] of credentialEnvMapping.entries()) {
      // cache and clear current value
      if (process.env[envVar]) {
        log.debug(`caching env variable ${envVar}`);
        cachedCredentials.set(envVar, process.env[envVar]);

        delete process.env[envVar];
      }
      if (credentials[key.toLowerCase()]) {
        // set env var to value if present in cred

        log.debug(`setting env variable ${envVar}`);
        process.env[envVar] = credentials[key.toLowerCase()];
      }
    }
  }

  let region = _.get(
    $,
    "item.turbot.custom.aws.regionName",
    _.get($, "item.turbot.metadata.aws.regionName", _.get($, "item.metadata.aws.regionName")),
  );

  if (!region) {
    // Guess from the partition which default region we should be, this crucial for
    // the setup where we run Turbot Master in AWS Commercial and we manage accounts in AWS GovCloud or AWS China
    // without this "default region" setup the default region will be the current region where Lambda is executing.
    // it's fine when the accounts are in the partition (All in commercial, all in GovCloud) but it will
    // fail miserably if the target account is in GovCloud/China while Turbot Master is in Commercial
    const defaultPartition = _.get($, "item.metadata.aws.partition", _.get($, "item.turbot.custom.aws.partition"));
    if (defaultPartition === "aws-us-gov") {
      region = "us-gov-west-1";
    } else if (defaultPartition === "aws-cn") {
      region = "cn-north-1";
    }
  }

  if (region) {
    for (const [, envVar] of regionEnvMapping.entries()) {
      // cache current value
      if (process.env[envVar]) {
        log.debug(`caching env variable ${envVar}`);
        cachedRegions.set(envVar, process.env[envVar]);
      }

      // set env var to region
      log.debug(`setting env variable ${envVar}`);
      process.env[envVar] = region;
    }
  }
};

const restoreCachedAWSEnvVars = () => {
  if (cachedCredentials.size > 0) {
    for (const [, envVar] of credentialEnvMapping.entries()) {
      if (process.env[envVar]) {
        delete process.env[envVar];
      }
    }
    for (const [envVar, value] of cachedCredentials.entries()) {
      process.env[envVar] = value;
    }
  }

  if (cachedRegions.size > 0) {
    for (const [, envVar] of regionEnvMapping.entries()) {
      if (process.env[envVar]) {
        delete process.env[envVar];
      }
    }
    for (const [envVar, value] of cachedRegions.entries()) {
      process.env[envVar] = value;
    }
  }
};

const initialize = async (event, context) => {
  const turbotOpts = {};

  // When in "turbot test" the lambda is being initiated directly, not via
  // SNS. In this case we short cut all of the extraction of credentials etc,
  // and just run directly with the input passed in the event.
  if (process.env.TURBOT_TEST) {
    turbotOpts.type = _.get(event, "meta.runType", process.env.TURBOT_FUNCTION_TYPE);

    // In test mode there is no metadata (e.g. AWS credentials) for Turbot,
    // they are all inherited from the underlying development environment.
    const turbot = new Turbot(event.meta || {}, turbotOpts);

    // In test mode, the input is in the payload of the event (no SNS wrapper).
    // default to using event directly for backwards compatibility
    turbot.$ = _.get(event, ["payload", "input"], event);

    // set the AWS credentials and region env vars using the values passed in the control input
    setAWSEnvVars(turbot.$);
    return { turbot };
  }

  // SNS sends a single record at a time to Lambda.
  const rawMessage = _.get(event, "Records[0].Sns.Message");
  if (!rawMessage) {
    throw errors.badRequest("Turbot controls should be called via SNS, or with TURBOT_TEST set to true", {
      event,
      context,
    });
  }

  return new Promise((resolve, reject) => {
    validator.validate(event.Records[0].Sns, async (err, snsMessage) => {
      if (err) {
        log.error("Error in validating SNS message", { error: err, message: event.Records[0].Sns });
        reject(errors.badRequest("Failed SNS message validation", { error: err, message: event.Records[0].Sns }));
        return;
      }

      let msgObj;
      try {
        msgObj = JSON.parse(snsMessage.Message);
      } catch (e) {
        log.error("Invalid input data while starting the lambda function. Message should be received via SNS", {
          error: e,
        });
        reject(
          errors.badRequest(
            "Invalid input data while starting the lambda function. Message should be received via SNS",
            {
              error: e,
            },
          ),
        );
        return;
      }

      log.info("Received message", {
        resourceId: _.get(msgObj, "meta.resourceId"),
        processId: _.get(msgObj, "meta.processId"),
        actionId: _.get(msgObj, "meta.actionId"),
        controlId: _.get(msgObj, "meta.controlId"),
        policyId: _.get(msgObj, "meta.policyValueId", _.get(msgObj, "meta.policyId")),
        tenant: _.get(msgObj, "meta.tenantId"),
        turbotVersion: _.get(msgObj, "meta.turbotVersion"),
      });

      try {
        const updatedMsgObj = await expandEventData(msgObj);

        // create the turbot object
        turbotOpts.senderFunction = messageSender;

        // Prefer the runType specified in the meta (for backward compatibility with anything prior to beta 46)
        turbotOpts.type = _.get(updatedMsgObj, "meta.runType", process.env.TURBOT_FUNCTION_TYPE);
        // if a function type was passed in the env vars use that
        if (!turbotOpts.type) {
          // otherwise default to control
          turbotOpts.type = "control";
        }

        const turbot = new Turbot(updatedMsgObj.meta, turbotOpts);
        // Convenient access
        turbot.$ = updatedMsgObj.payload.input;

        // set the AWS credentials and region env vars using the values passed in the control input
        setAWSEnvVars(turbot.$);

        resolve({ turbot });
      } catch (error) {
        reject(error);
      }
    });
  });
};

const expandEventData = async (msgObj) => {
  const payloadType = _.get(msgObj, "payload.type");
  if (payloadType !== "large_parameter") {
    return msgObj;
  }

  const tmpDir = await new Promise((resolve, reject) => {
    tmp.dir({ keep: true }, (err, path) => {
      if (err) reject(err);
      else resolve(path);
    });
  });

  try {
    const largeParameterZipUrl = msgObj.payload.s3PresignedUrlForParameterGet;
    const largeParamFileName = path.resolve(tmpDir, "large-parameter.zip");

    // Download the large parameter zip file
    const file = fs.createWriteStream(largeParamFileName);
    const downloadStream = got.stream(largeParameterZipUrl);

    await new Promise((resolve, reject) => {
      downloadStream.on("error", (err) => {
        console.error("Error downloading large parameter", {
          url: largeParameterZipUrl,
          error: err,
        });
        reject(err);
      });

      file.on("error", (err) => {
        console.error("Error writing large parameter file", {
          file: largeParamFileName,
          error: err,
        });
        reject(err);
      });

      file.on("finish", () => {
        console.log("Large parameter file downloaded successfully", { largeParamFileName });
        resolve();
      });

      downloadStream.pipe(file);
    });

    // Extract the zip file
    const extract = require("extract-zip");
    await extract(largeParamFileName, { dir: tmpDir });

    // Parse the large input JSON
    const parsedData = await fs.readJson(path.resolve(tmpDir, "large-input.json"));

    // Clean up temp directory
    rimraf.sync(tmpDir);

    // Merge the parsed data with the message object
    _.defaultsDeep(msgObj.payload, parsedData.payload);

    return msgObj;
  } catch (error) {
    // Clean up temp directory on error
    if (tmpDir) {
      rimraf.sync(tmpDir);
    }
    throw error;
  }
};

/**
 * The callback here is the Lambda's callback. When it's called the lambda will be terminated
 */
const messageSender = async (message, opts, callback) => {
  const snsArn = message.meta.returnSnsArn;

  const params = {
    Message: JSON.stringify(message),
    MessageAttributes: {},
    TopicArn: snsArn,
  };

  console.log("Publishing to SNS with params new", { params });

  const paramToUse =
    _mode === "container"
      ? _containerSnsParam
      : {
          credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
            sessionToken: process.env.AWS_SESSION_TOKEN,
          },
          region: process.env.AWS_REGION,
          maxAttempts: 4, // Equivalent to maxRetries in v3
          retryStrategy: new taws.CustomDiscoveryRetryStrategy(4), // Assuming this is a custom function
        };

  console.log("Publishing to SNS with paramToUse new", { paramToUse });
  const sns = taws.connect(SNSClient, paramToUse);

  const command = new PublishCommand(params);

  log.info("messageSender: publish to SNS", {
    snsArn,
    actionId: _.get(message, "meta.actionId"),
    controlId: _.get(message, "meta.controlId"),
    policyId: _.get(message, "meta.policyValueId", _.get(message, "meta.policyId")),
    paramToUse,
  });

  try {
    const results = await sns.send(command);
    log.info("SNS message published", { results });

    if (callback) {
      return callback(null, results);
    }
    return results;
  } catch (xErr) {
    log.error("Error publishing commands to SNS", { error: xErr });
    if (callback) {
      return callback(xErr);
    }
    throw xErr;
  }
};

const persistLargeCommands = async (cargoContainer, opts) => {
  let largeCommands;
  if (cargoContainer.largeCommandV2) {
    largeCommands = {
      commands: cargoContainer.commands,
      logEntries: cargoContainer.logEntries,
    };
  } else {
    largeCommands = cargoContainer.largeCommands;
  }

  if (_.isEmpty(largeCommands)) {
    cargoContainer.largeCommandState = "finalised";
    return;
  }

  const osTempDir = _.isEmpty(process.env.TURBOT_TMP_DIR) ? os.tmpdir() : process.env.TURBOT_TMP_DIR;
  const tmpDir = `${osTempDir}/commands`;

  try {
    await fs.access(tmpDir);
  } catch (err) {
    if (err.code === "ENOENT") {
      opts.log.debug("Temporary directory does not exist. Creating ...", { modDir: tmpDir });
      await fs.ensureDir(tmpDir);
    } else {
      throw err;
    }
  }

  // Create the large command zip
  const outputStreamBuffer = new streamBuffers.WritableStreamBuffer({
    initialSize: 1000 * 1024, // start at 1000 kilobytes.
    incrementAmount: 1000 * 1024, // grow by 1000 kilobytes each time buffer overflows.
  });

  const archiver = require("archiver");
  const archive = archiver("zip", {
    zlib: { level: 9 }, // Sets the compression level.
  });
  archive.pipe(outputStreamBuffer);

  archive.append(JSON.stringify(largeCommands), { name: "large-commands.json" });

  const zipFilePath = await new Promise((resolve, reject) => {
    outputStreamBuffer.on("finish", () => {
      const zipFilePath = path.resolve(tmpDir, `${opts.processId}.zip`);
      fs.writeFile(zipFilePath, outputStreamBuffer.getContents(), (err) => {
        if (err) reject(err);
        else resolve(zipFilePath);
      });
    });

    archive.on("error", reject);
    archive.finalize();
  });

  // Upload the large commands to S3
  const stream = fs.createReadStream(zipFilePath);
  const stat = await fs.stat(zipFilePath);

  const urlOpts = url.parse(opts.s3PresignedUrl);
  const reqOptions = {
    method: "PUT",
    host: urlOpts.host,
    path: urlOpts.path,
    headers: {
      "content-type": "application/zip",
      "content-length": stat.size,
      "content-encoding": "zip",
      "cache-control": "public, no-transform",
    },
  };

  opts.log.debug("Options to put large commands", { options: reqOptions });
  log.info("Saving large command with options", { options: reqOptions });

  await new Promise((resolve, reject) => {
    const req = https
      .request(reqOptions, (resp) => {
        let data = "";

        // Do not remove this block, somehow request does not complete if I remove ths (?)
        resp.on("data", (chunk) => {
          data += chunk;
        });

        resp.on("end", () => {
          opts.log.debug("End put large commands", { data: data });
          log.info("Large command saving completed", { data: data });
          resolve();
        });
      })
      .on("error", (err) => {
        console.error("Error putting commands to S3", { error: err });
        reject(err);
      });

    stream.pipe(req);
  });

  // Clean up temp directory
  if (tmpDir) {
    rimraf.sync(tmpDir);
  }

  log.info("Cargo state set to finalized no further data will be added.");
  cargoContainer.largeCommandState = "finalised";
};

const finalize = async (event, context, init, err, result) => {

  if (_mode === "container") {
    delete process.env.AWS_ACCESS_KEY;
    delete process.env.AWS_ACCESS_KEY_ID;
    delete process.env.AWS_SECRET_KEY;
    delete process.env.AWS_SECRET_ACCESS_KEY;
    delete process.env.AWS_SESSION_TOKEN;
    delete process.env.AWS_SECURITY_TOKEN;
  } else {
    // restore the cached credentials and region values
    restoreCachedAWSEnvVars();
  }

  if (!init || !init.turbot) {
    // can't do anything here .. have to just silently return
    console.error("Error reported but no turbot object, unable to send anything back", { error: err });
  }

  // DO NOT log error here - we've persisted the large commands, let's avoid adding
  // any new information into the cargo

  // Do not wait for empty callback look to terminate the process
  context.callbackWaitsForEmptyEventLoop = false;

  // If in test mode, then do not publish to SNS. Instead, morph the response to include
  // both the turbot information and the raw result so they can be used for assertions.
  if (process.env.TURBOT_TEST) {
    // include process event with result

    // get the function result as a process event
    const processEvent = init.turbot.sendFinal();
    result = { result, turbot: processEvent };

    if (err) {
      // if there is an error, lambda does not return the result, so include it with the error
      // lambda returns a standard error object so to pass a custom object we must stringify
      const utils = require("@turbot/utils");
      return JSON.stringify(utils.data.sanitize({ err, result }, { breakCircular: true }));
    }
    return result;
  }

  // On error we just send back all the existing data. Lambda will re-try 3 times, the receiving end (Turbot Core) will
  // receive the same log 3 times (providing it's the same execution). On the receiving end (Turbot Core) we
  // will detect if the same error happens 3 time and terminate the process.
  //
  // NOTE: we should time limit the Lambda execution to stop running Lambda costing us $$$

  init.turbot.stop();
  if (!err) {
    try {
      await new Promise((resolve, reject) => {
        init.turbot.sendFinal((_err) => {
          if (_err) {
            console.error("Error in send final function", { error: _err });
            reject(_err);
          } else {
            resolve();
          }
        });
      });
      return null;
    } catch (_err) {
      console.error("Error in send final function", { error: _err });
      // if there's an error in the sendFinal function ... that means our SNS message may not
      // make it back to Turbot Worker, so we need to retry and return the error.

      // Lambda will retries 2 times then it will end up in the DLQ. If we don't return the error (previous version of the code)
      // we will end up as "missing" control run -> we don't send the result back to Turbot Server
      // but Lambda doesn't retru.
      throw _err;
    }
  }

  // Don't do this for Lambda, see comment above
  if (_mode === "container") {
    init.turbot.log.error("Error running container", { error: err });
    init.turbot.error("Error running container");
  }

  try {
    await new Promise((resolve, reject) => {
      init.turbot.send((_err) => {
        if (_err) {
          console.error("Error in send function", { error: _err });
          reject(_err);
        } else {
          resolve();
        }
      });
    });
    return null;
  } catch (_err) {
    console.error("Error in send function", { error: _err });
    throw err;
  }
};

// Container specific - no issue with multiple Lambda functions running
// at the same time
let _event, _context, _init, _mode;

function gfn(asyncHandler) {
  _mode = "lambda";

  // Return a function in Lambda signature format
  return async (event, context) => {
    try {
      // Initialize the Turbot metadata and context, configuring the lambda function
      // for simpler writing and use by mods.
      const init = await initialize(event, context);

      _event = event;
      _context = context;
      _init = init;

      try {
        // Run the handler function directly as async
        let result = await asyncHandler(init.turbot, init.turbot.$);
        let finalResult = result;
        let finalError = null;

        if (result && result.fatal) {
          if (_.get(init, "turbot")) {
            init.turbot.log.error(
              `Unexpected fatal error while executing Lambda/Container function. Container error is always fatal. Execution will be terminated immediately.`,
              {
                error: result,
                mode: _mode,
              },
            );
          }

          // for a fatal error, set control state to error and return a null error
          // so SNS will think the lambda execution is successful and will not retry
          finalResult = init.turbot.error(result.message, { error: result });
          finalError = null;
        } else if (result && result.message) {
          // If we receive error we want to add it to the turbot object.
          init.turbot.log.error(
            `Unexpected non-fatal error while executing Lambda function. Lambda will be retried based on AWS Lambda retry policy`,
            {
              error: result,
              mode: _mode,
            },
          );
          finalError = result;
        }

        await persistLargeCommands(init.turbot.cargoContainer, {
          log: init.turbot.log,
          s3PresignedUrl: init.turbot.meta.s3PresignedUrlLargeCommands,
          processId: init.turbot.meta.processId,
        });

        // Finalize handling
        await finalize(event, context, init, finalError, finalResult);
      } catch (err) {
        console.error("Caught exception while executing the handler", { error: err, event, context });
        await finalize(event, context, init, err, null);
      }
    } catch (err) {
      throw err;
    }
  };
}

const unhandledExceptionHandler = async (err) => {
  if (err) {
    err.fatal = true;
  }
  await finalize(_event, _context, _init, err, null);
};

/**
 * AWS added their own unhandledRejection for Node 10 Lambda (!)
 *
 * Added all the others to ensure that our wrapper is the only one adding the the following events.
 *
 * https://forums.aws.amazon.com/thread.jspa?messageID=906365&tstart=0
 */
process.removeAllListeners("SIGINT");
process.removeAllListeners("SIGTERM");
process.removeAllListeners("uncaughtException");
process.removeAllListeners("unhandledRejection");

process.on("SIGINT", async (e) => {
  log.error("Lambda process received SIGINT", { error: e });
  await unhandledExceptionHandler(e);
});

process.on("SIGTERM", async (e) => {
  log.error("Lambda process received SIGTERM", { error: e });
  await unhandledExceptionHandler(e);
});

process.on("uncaughtException", async (e) => {
  log.error("Lambda process received Uncaught Exception", { error: e });
  await unhandledExceptionHandler(e);
});

process.on("unhandledRejection", async (e) => {
  log.error("Lambda process received Unhandled Rejection, do not ignore", { error: e });
  await unhandledExceptionHandler(e);
});

const decryptContainerParameters = async ({ envelope }) => {
  const crypto = require("crypto");
  const ALGORITHM = "aes-256-gcm";

  const params = {
    KeyId: envelope.kmsKey,
    CiphertextBlob: Buffer.from(envelope.$$dataKey, "base64"),
    EncryptionContext: { purpose: "turbot-control" },
  };

  // Create a KMS client using AWS SDK v3
  const kms = taws.connect(KMSClient, params);
  const command = new DecryptCommand(params);

  // Decrypt the data key
  const data = await kms.send(command);
  const decryptedEphemeralDataKey = data.Plaintext.toString("utf8");

  // Decrypt the actual data
  const plaintextEncoding = "utf8";
  const keyBuffer = Buffer.from(decryptedEphemeralDataKey, "base64");
  const cipherBuffer = Buffer.from(envelope.$$data, "base64");
  const ivBuffer = cipherBuffer.slice(0, 12);
  const chunk = cipherBuffer.slice(12, -16);
  const tagBuffer = cipherBuffer.slice(-16);
  const decipher = crypto.createDecipheriv(ALGORITHM, keyBuffer, ivBuffer);
  decipher.setAuthTag(tagBuffer);
  const plaintext = decipher.update(chunk, null, plaintextEncoding) + decipher.final(plaintextEncoding);

  const paramObj = JSON.parse(plaintext);
  return paramObj;
};

class Run {
  constructor() {
    _mode = "container";

    this._runnableParameters = process.env.TURBOT_CONTROL_CONTAINER_PARAMETERS;

    if (_.isEmpty(this._runnableParameters) || this._runnableParameters === "undefined") {
      log.error("No parameters supplied", this._runnableParameters);
      throw new errors.badRequest("No parameters supplied", this._runnableParameters);
    }
  }

  async run() {
    try {
      // Retrieve the container run parameters
      const response = await got(this._runnableParameters, {
        timeout: {
          request: 10000,
        },
        decompress: true,
        responseType: "json",
      });
      const rawLaunchParameters = response.body;

      // Decrypt parameters if needed
      let launchParameters = rawLaunchParameters;
      if (rawLaunchParameters.$$dataKey) {
        launchParameters = await decryptContainerParameters({ envelope: rawLaunchParameters });
      }

      // Get container metadata for EC2 launch type
      let containerMetadata;
      if (launchParameters.meta.launchType === "EC2") {
        try {
          const metadataResponse = await got(
            `http://169.254.170.2${process.env.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI}`,
            {
              responseType: "json",
            },
          );
          containerMetadata = metadataResponse.body;
          _containerSnsParam = {
            credentials: {
              accessKeyId: containerMetadata.AccessKeyId,
              secretAccessKey: containerMetadata.SecretAccessKey,
              sessionToken: containerMetadata.Token,
            },
            region: process.env.TURBOT_REGION,
            maxAttempts: 4, // Equivalent to maxRetries in v3
            retryStrategy: new taws.CustomDiscoveryRetryStrategy(4), // Assuming this is a custom function
          };
        } catch (err) {
          // Handle metadata retrieval error
          log.warning("Failed to retrieve container metadata", { error: err });
        }
      }

      // Create the turbot object
      const turbotOpts = {
        senderFunction: messageSender,
      };
      const turbot = new Turbot(launchParameters.meta, turbotOpts);
      turbot.$ = launchParameters.payload.input;

      // Set up caches
      _event = {};
      _context = {};
      _init = {
        turbot: turbot,
      };

      // Set AWS environment variables and run the handler
      setAWSEnvVars(launchParameters.payload.input);
      await this.handler(turbot, launchParameters.payload.input);

      // Clean up environment variables for container
      log.debug(
        "Deleting env variables: AWS_ACCESS_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_SECURITY_TOKEN",
      );

      delete process.env.AWS_ACCESS_KEY;
      delete process.env.AWS_ACCESS_KEY_ID;
      delete process.env.AWS_SECRET_KEY;
      delete process.env.AWS_SECRET_ACCESS_KEY;
      delete process.env.AWS_SESSION_TOKEN;
      delete process.env.AWS_SECURITY_TOKEN;

      // Persist large commands
      await persistLargeCommands(turbot.cargoContainer, {
        log: turbot.log,
        s3PresignedUrl: turbot.meta.s3PresignedUrlLargeCommands,
        processId: turbot.meta.processId,
      });

      // Finalize container run
      log.debug("Finalize in container");
      turbot.stop();
      await new Promise((resolve) => {
        turbot.sendFinal(() => {
          resolve();
        });
      });

      process.exit(0);
    } catch (err) {
      log.error("Error while running", { error: err });

      if (_init && _init.turbot) {
        _init.turbot.log.error("Error while running container", { error: err });
        _init.turbot.error("Error while running container");
        _init.turbot.stop();
        await new Promise((resolve) => {
          _init.turbot.sendFinal(() => {
            resolve();
          });
        });
      } else {
        await finalize(_event, _context, _init, err, null, () => {
          console.error("Error in finalizing the container run due to error", { error: err });
        });
      }

      process.exit(0);
    }
  }

  async handler(turbot, $) {
    log.warning("Base class handler is called, nothing to do");
  }
}

gfn.fn = gfn;

// Generic runner
gfn.Run = Run;

module.exports = gfn;
