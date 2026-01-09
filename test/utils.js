const { assert } = require("chai");
const { get, isEmpty, defaultsDeep } = require("../utils");

describe("utils", function () {
  describe("get", function () {
    it("gets nested property with array path", function () {
      const obj = { a: { b: { c: 1 } } };
      assert.equal(get(obj, ["a", "b", "c"]), 1);
    });

    it("gets nested property with string path", function () {
      const obj = { a: { b: { c: 1 } } };
      assert.equal(get(obj, "a.b.c"), 1);
    });

    it("handles array index in string path", function () {
      const obj = { Records: [{ Sns: { Message: "hello" } }] };
      assert.equal(get(obj, "Records[0].Sns.Message"), "hello");
    });

    it("returns default for missing property", function () {
      const obj = { a: 1 };
      assert.equal(get(obj, "b.c", "default"), "default");
    });

    it("returns default for null object", function () {
      assert.equal(get(null, "a.b", "default"), "default");
    });

    it("returns default for undefined property", function () {
      const obj = { a: undefined };
      assert.equal(get(obj, "a", "default"), "default");
    });

    it("handles nested default value", function () {
      const obj = { a: { b: 1 } };
      const result = get(obj, "c.d", get(obj, "a.b"));
      assert.equal(result, 1);
    });
  });

  describe("isEmpty", function () {
    it("returns true for null", function () {
      assert.isTrue(isEmpty(null));
    });

    it("returns true for undefined", function () {
      assert.isTrue(isEmpty(undefined));
    });

    it("returns true for empty string", function () {
      assert.isTrue(isEmpty(""));
    });

    it("returns false for non-empty string", function () {
      assert.isFalse(isEmpty("hello"));
    });

    it("returns true for empty array", function () {
      assert.isTrue(isEmpty([]));
    });

    it("returns false for non-empty array", function () {
      assert.isFalse(isEmpty([1, 2, 3]));
    });

    it("returns true for empty object", function () {
      assert.isTrue(isEmpty({}));
    });

    it("returns false for non-empty object", function () {
      assert.isFalse(isEmpty({ a: 1 }));
    });

    it("returns false for numbers", function () {
      assert.isFalse(isEmpty(0));
      assert.isFalse(isEmpty(42));
    });
  });

  describe("defaultsDeep", function () {
    it("merges nested objects", function () {
      const target = { a: { b: 1 } };
      const source = { a: { c: 2 }, d: 3 };
      defaultsDeep(target, source);
      assert.deepEqual(target, { a: { b: 1, c: 2 }, d: 3 });
    });

    it("does not override existing values", function () {
      const target = { a: { b: 1 } };
      const source = { a: { b: 2 } };
      defaultsDeep(target, source);
      assert.equal(target.a.b, 1);
    });

    it("handles null source", function () {
      const target = { a: 1 };
      defaultsDeep(target, null);
      assert.deepEqual(target, { a: 1 });
    });

    it("handles arrays by not merging them", function () {
      const target = { a: [1, 2] };
      const source = { a: [3, 4] };
      defaultsDeep(target, source);
      assert.deepEqual(target.a, [1, 2]);
    });
  });
});
