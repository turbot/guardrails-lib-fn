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
});