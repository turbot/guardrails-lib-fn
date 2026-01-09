/**
 * Native replacements for lodash functions.
 * These utilities replace lodash to reduce bundle size.
 */

/**
 * Get a nested property from an object safely.
 * Supports both dot notation strings ("a.b.c") and array paths (["a", "b", "c"]).
 * Also supports bracket notation for arrays in strings ("Records[0].Sns.Message").
 *
 * @param {Object} obj - The object to query
 * @param {string|Array} path - The path of the property to get
 * @param {*} defaultValue - The value returned if the resolved value is undefined
 * @returns {*} The resolved value
 */
function get(obj, path, defaultValue) {
  if (obj == null) return defaultValue;

  // Convert string path to array
  // Handles both "a.b.c" and "a[0].b" notation
  const keys = Array.isArray(path)
    ? path
    : path.replace(/\[(\d+)\]/g, ".$1").split(".");

  let result = obj;
  for (const key of keys) {
    if (result == null) return defaultValue;
    result = result[key];
  }

  return result === undefined ? defaultValue : result;
}

/**
 * Check if a value is empty.
 * Returns true for null, undefined, empty strings, empty arrays, and empty objects.
 *
 * @param {*} value - The value to check
 * @returns {boolean} True if the value is empty
 */
function isEmpty(value) {
  if (value == null) return true;
  if (typeof value === "string") return value.length === 0;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === "object") return Object.keys(value).length === 0;
  return false;
}

/**
 * Deep merge source into target, only setting values that are undefined in target.
 * This is a simplified version that handles the specific use case in this codebase.
 *
 * @param {Object} target - The target object
 * @param {Object} source - The source object
 * @returns {Object} The target object
 */
function defaultsDeep(target, source) {
  if (!source || typeof source !== "object") return target;
  if (!target || typeof target !== "object") return source;

  for (const key of Object.keys(source)) {
    const sourceVal = source[key];
    const targetVal = target[key];

    if (sourceVal && typeof sourceVal === "object" && !Array.isArray(sourceVal)) {
      // Recursively merge objects
      if (!targetVal || typeof targetVal !== "object") {
        target[key] = {};
      }
      defaultsDeep(target[key], sourceVal);
    } else if (targetVal === undefined) {
      // Only set if target doesn't have this key
      target[key] = sourceVal;
    }
  }

  return target;
}

module.exports = { get, isEmpty, defaultsDeep };
