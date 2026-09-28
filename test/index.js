const { assert } = require("chai");
const gfn = require(".."); // your index.js

describe("@turbot/gfn", function () {
  this.timeout(5000);

  before(() => { process.env.TURBOT_TEST = true; });
  after(() => { delete process.env.TURBOT_TEST; });

  // Minimal event so initialize() can build Turbot without SNS
  const event = {
    meta: { runType: "control", processId: "p-1" },
    payload: { input: {} },
  };

  it("has turbot variable", async function () {
    const wrapped = gfn(async (turbot, $) => {
      assert.exists(turbot, "turbot should be passed");
      assert.isFunction(turbot.ok, "turbot.ok should be a function");
      assert.isObject(turbot.resource, "turbot.resource should exist");
      assert.isFunction(turbot.resource.create, "turbot.resource.create should be a function");
      turbot.ok(); // mark success
      return true;
    });

    await wrapped(event, {}); // (event, context)
  });

  it("turbot.ok works", async function () {
    const wrapped = gfn(async (turbot, $) => {
      turbot.ok(); // should not throw
      return true;
    });

    await wrapped(event, {});
  });

  describe("resolved value in test mode", function () {
    const controlEvent = {
      meta: { runType: "control", processId: "p-2", controlId: "c-2" },
      payload: { input: {} },
    };

    it("resolves { result, turbot } with the process event on success", async function () {
      const wrapped = gfn(async (turbot) => {
        turbot.ok("all good");
        return { answer: 42 };
      });

      const response = await wrapped(controlEvent, {});

      assert.isObject(response, "wrapper should resolve the finalize() output, not undefined");
      assert.deepEqual(response.result, { answer: 42 });
      assert.equal(response.turbot.type, "process.turbot.com:terminate");
      assert.equal(response.turbot.meta.processId, "p-2");
      const commands = response.turbot.payload.commands;
      assert.lengthOf(commands, 1);
      assert.equal(commands[0].type, "control_update");
      assert.equal(commands[0].meta.controlId, "c-2");
      assert.equal(commands[0].payload.state, "ok");
      assert.equal(commands[0].payload.reason, "all good");
    });

    it("resolves a JSON string with the error and process event when the handler throws", async function () {
      const wrapped = gfn(async (turbot) => {
        turbot.log.info("before failure");
        throw new Error("handler failed");
      });

      const response = await wrapped(controlEvent, {});

      assert.isString(response);
      const parsed = JSON.parse(response);
      assert.equal(parsed.err.message, "handler failed");
      assert.equal(parsed.result.turbot.type, "process.turbot.com:terminate");
      assert.equal(parsed.result.turbot.meta.processId, "p-2");
    });
  });

  describe("Lambda (SNS) mode", function () {
    const { SNSClient } = require("@aws-sdk/client-sns");
    const MessageValidator = require("@turbot/sns-validator");
    const originalSend = SNSClient.prototype.send;
    const originalValidate = MessageValidator.prototype.validate;
    const returnSnsArn = "arn:aws:sns:us-east-1:123456789012:turbot-return";
    let published;
    let publishError;

    const snsEvent = (msg) => ({
      Records: [{ Sns: { Type: "Notification", MessageId: "m-1", Message: JSON.stringify(msg) } }],
    });

    const message = {
      meta: { runType: "control", processId: "p-3", controlId: "c-3", returnSnsArn },
      payload: { input: {} },
    };

    before(function () {
      delete process.env.TURBOT_TEST;
      MessageValidator.prototype.validate = function (hash, cb) {
        cb(null, hash);
      };
      SNSClient.prototype.send = async function (command) {
        published.push(command.input);
        if (publishError) {
          throw publishError;
        }
        return { MessageId: `published-${published.length}` };
      };
    });

    after(function () {
      SNSClient.prototype.send = originalSend;
      MessageValidator.prototype.validate = originalValidate;
      process.env.TURBOT_TEST = true;
    });

    beforeEach(function () {
      published = [];
      publishError = null;
    });

    it("publishes the final process event to the return SNS topic and resolves null", async function () {
      const wrapped = gfn(async (turbot) => {
        turbot.ok("all good");
        return { answer: 42 };
      });

      const response = await wrapped(snsEvent(message), {});

      assert.isNull(response);
      assert.lengthOf(published, 1);
      assert.equal(published[0].TopicArn, returnSnsArn);
      const processEvent = JSON.parse(published[0].Message);
      assert.equal(processEvent.type, "process.turbot.com:terminate");
      assert.equal(processEvent.meta.processId, "p-3");
      assert.equal(processEvent.payload.commands[0].payload.state, "ok");
    });

    it("publishes the process event and rejects with the error when the handler throws", async function () {
      const wrapped = gfn(async () => {
        throw new Error("handler failed");
      });

      let caught;
      try {
        await wrapped(snsEvent(message), {});
      } catch (e) {
        caught = e;
      }

      assert.exists(caught, "wrapper should reject so Lambda retries");
      assert.equal(caught.message, "handler failed");
      assert.lengthOf(published, 1);
      assert.equal(published[0].TopicArn, returnSnsArn);
    });

    it("rejects when publishing the final process event fails", async function () {
      publishError = new Error("SNS unavailable");
      const wrapped = gfn(async (turbot) => {
        turbot.ok("all good");
      });

      let caught;
      try {
        await wrapped(snsEvent(message), {});
      } catch (e) {
        caught = e;
      }

      assert.exists(caught, "wrapper should reject so Lambda retries");
      assert.equal(caught.message, "SNS unavailable");
    });
  });
});