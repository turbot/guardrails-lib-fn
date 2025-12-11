# Turbot Guardrails Lib Fn

# Release History

## 1.0.4 [2025-12-11]

Fixed: Control stuck on Running state for resource type targets account on gov and china cloud.

## 1.0.3 [2025-09-17]

Fixed: Resolved an issue where controls encountering non-fatal errors were not retried and remained in the handling state indefinitely.

## 1.0.2 [2025-09-17]

Fixed: Controls no longer stuck in handling state.

## 1.0.1 [2025-09-10]

Fixed: Controls now properly transition to error state when exceptions occur, allowing the system to handle failures gracefully. [#4](https://github.com/turbot/guardrails-lib-fn/issues/4)


## 1.0.0 [2025-08-22]

Initial 5.0.0 release.
