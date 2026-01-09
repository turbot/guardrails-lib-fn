const { assert } = require("chai");
const gfn = require(".."); // your index.js

describe("@turbot/gfn", function () {
  this.timeout(5000);

  before(() => { process.env.TURBOT_TEST = true; });
  after(() => { delete process.env.TURBOT_TEST; });

  describe("unhandled exception handling", function () {
    it("should not crash when unhandledRejection occurs outside Lambda invocation", async function () {
      // This tests the fix for issue #25 - crash loop when unhandled rejection
      // occurs outside Lambda invocation context.
      //
      // Before the fix, finalize() would crash with:
      // "Cannot set properties of undefined (setting 'callbackWaitsForEmptyEventLoop')"
      // when context was undefined, causing an infinite crash loop.
      //
      // The fix adds:
      // 1. Early return when init/turbot is undefined
      // 2. Guard on context before accessing callbackWaitsForEmptyEventLoop
      //
      // This test verifies the handler completes without throwing. If this test
      // passes, it proves finalize() handles the edge case gracefully.

      const testError = new Error("Test unhandled rejection");

      // Emit the event - before the fix, this would crash and cause infinite loop.
      // After the fix, it should complete gracefully.
      process.emit("unhandledRejection", testError);

      // Give async handler time to execute
      await new Promise((resolve) => setTimeout(resolve, 100));

      // If we reach here without the test framework catching an uncaught exception,
      // the fix is working. The test passing IS the assertion.
      assert.ok(true, "unhandledRejection handler completed without crashing");
    });
  });

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
});