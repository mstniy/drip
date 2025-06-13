## 0.2.0 - 2025-06-13

- Support for change stream replace events
- Removed the cluster time from the CEA cursor
- CSEvent now has a separate field for cluster time
- dripCEAResume: Tolerate resuming the "head" of the PCS
- dripCEAResume: Fix rejectIfOlderThan if the cursor is not on a nop
- Stop combining adjacent $match stages when inverting pipelines
- Persister: Avoid persisting all the changes in the cluster time, which used to lead to feedback loops.
- Support for Bun
- CC: Attach a comment to the MongoDB query
- Fix bug in rejectIfOlderThan logic

## 0.1.0 - 2025-04-18

- Avoid using `$or` for tuple comparison. This sometimes confused MongoDB into using an in-memory sort, even if there is an index supporting the sort order. Instead, use multiple individual cursors and append the results.
- Reduce client traffic by only returning the noop event with the greatest cluster time, if it is more recent than all the other events, if any exist.
- Reduce database traffic by inverting the sync pipeline to distinguish updates from additions in select cases.
- Reduce database load by stripping away stages of the synced pipeline if possible.
- Reduce PCS size by not storing PCS event version in the PCS events themselves.
- Reduce PCS size by only storing the wall clock for noop events.
- Removed buggy support for `$redact`.
- Fixed buggy scoping for `$replaceRoot` and `$replaceWith`.
- Persister: Return an async generator. Allows it to be stopped gracefully.
- CEA: Support for refusing too old logical cursors. Useful for eventually removing old persisted change events without risking ongoing CEAs being affected.
- Added cleaner: Removes persisted change stream events older than a specified date.
- CEA: Removed some stages that cause MongoDB to always do an in-memory sort, even if there is an index supporting the sort order.
- Added support for processing pipelines. They run after the $sort stage but cannot filter out documents. Useful in cases where MongoDB resorts to an in-memory sort unnecessarily.
- CC: Return a lower bound for the cluster time at the beginning of CC.
- CC: Use causal consistency. This ensures CC never reads data older than the lower bound it returned.
- CEA: Take as parameter the lower bound for the cluster time at the beginning of CC instead of a wall clock. More efficient and does not assume that the client wall clock is accurate.
- Persister: Use bulk insertions and transactions to improve throughput.
- Close MongoDB cursors correctly.
- CC: Throw if there are no prior persisted events.

## 0.0.1 - 2025-03-15

- Initial version
