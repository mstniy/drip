## 0.1.0

- Reduce client traffic by only returning the noop event with the greatest cluster time, if it is more recent than all the other events, if any exist.
- Reduce database traffic by inverting the synced pipeline to distinguish updates from additions, if clearly beneficial.
- Reduce database load by stripping away stages of the synced pipeline if possible.
- Reduce PCS size by not storing PCS event version in the PCS events themselves.
- Reduce PCS size by only storing the wall clock for noop events.
- Removed buggy support for `$redact`.
- Fixed buggy scoping for `$replaceRoot` and `$replaceWith`.

## 0.0.1 - 2025-03-15

- Initial version
