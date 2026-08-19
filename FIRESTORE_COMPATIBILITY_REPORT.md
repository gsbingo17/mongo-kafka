# MongoDB Kafka Connector vs Firestore Enterprise Change Streams

Compatibility report for the MongoDB Kafka Connector (3.0.1-SNAPSHOT) run against a Firestore
Enterprise MongoDB-compatible endpoint.

The connector itself is **unmodified** — nothing under `src/main/` was touched. Only integration
test setup, teardown and connection wiring were adapted, under the invariant that no adaptation may
turn a failing test into a passing one. The harness documentation lists every change and the
category it falls in.

This document is in two parts:

* **Part A** (this half) is the hand-written analysis — what works, what does not, and why.
* **Part B** is the mandated per-test record, one entry per test. It is **generated** by
  `python3 generate-report.py fs-results8` from the archived JUnit XML. Do not hand-edit it; change
  the script's `ATTRIBUTION` / `CAUSES` tables instead.

Results below are from the sweep archived in `fs-results8/`, run with collection-scope change
streams **provisioned** (`FS_PROJECT` set) and database-scope forcing **on**
(`FS_FORCE_DATABASE_SCOPE=true`). Provisioning is what gives this run real coverage: the 25 tests
earlier sweeps skipped all executed and returned verdicts.

It supersedes `fs-results7`. Two things changed. `testStartAtOperationTime` now **passes**: it had
been replaying from `Instant.EPOCH` into change stream history left by earlier runs, and the
harness's declared deviation moved its start time to the clock. And `MongoSourceTaskIntegrationTest`
was **re-run** within this sweep: on the first pass five of its tests died inside the harness's
provisioner on an expired access token, before reaching the connector. That is a harness defect, not
a Firestore signal, so the class was re-run after the provisioner was fixed to re-mint and retry on
a 401. All five then returned real verdicts — one passed, four failed for the reasons recorded
below. No other class was re-run.

---

## Headline

| | Tests | Share |
|---|---:|---:|
| **PASS** | 31 | 38% |
| **FAIL** | 33 | 40% |
| **BLOCKED** (not executed) | 18 | 22% |
| **Total** | **82** | |

| Class | Tests | PASS | FAIL | BLOCKED |
|---|---:|---:|---:|---:|
| `MongoSinkConnectorIntegrationTest` | 7 | 7 | 0 | 0 |
| `ConnectorValidationIntegrationTest` | 23 | 8 | 3 | 12 |
| `ChangeStreamRoundTripTest` | 3 | 2 | 0 | 1 |
| `FullDocumentRoundTripIntegrationTest` | 5 | 3 | 2 | 0 |
| `MongoSinkTaskIntegrationTest` | 11 | 3 | 8 | 0 |
| `MongoSourceTaskIntegrationTest` | 24 | 6 | 16 | 2 |
| `MongoSourceConnectorIntegrationTest` | 9 | 2 | 4 | 3 |

Compared with the last unprovisioned sweep (`fs-results5`: 21 / 18 / 43), provisioning moved 25
tests out of BLOCKED: 9 of them passed and 16 failed.

**One of the 33 FAILs is not a Firestore signal** — it is an artifact of pinning every test to one
database, which Firestore forces because it cannot create databases on demand. That leaves
**32 real compatibility findings, and 19 of them are the same one**: unsorted reads are not in
insertion order.

One further real difference appears in **no** test result, because nothing asserts on it: Firestore
omits `operationTime` from command replies and its resume tokens are not in MongoDB's `_data`
format, so the connector's replication-lag metric cannot be computed. See §6.

### Why each test did not pass

| Cause | Tests | Verdict |
|---|---:|---|
| Unsorted reads are not in insertion order | 19 | **Real difference** — one root cause, over half the failures |
| User/role creation is IAM-administered | 9 | **Untestable over the wire protocol** |
| `collStats` omits `nindexes` (time series) | 7 | **Real difference** (plus a connector robustness bug) |
| Test needs >1 distinct database | 5 | **Out of scope** — Firestore holds one database |
| Pre-existing connector version gate | 4 | **Unrelated to Firestore** |
| Stale resume token reported with the wrong error code | 1 | **Real difference** — breaks `errors.tolerance=all` recovery |
| Disallowed pipeline stage reported with the wrong error code | 1 | **Real difference** — connector stalls instead of failing fast |
| `changeStreamPreAndPostImages` unsupported | 1 | **Real gap** |
| `showExpandedEvents` unsupported | 1 | **Real gap** |
| `updateDescription.truncatedArrays` not populated | 1 | **Real payload difference** |
| Document size limit below 16 MB | 1 | **Real difference** |
| Schema Registry subject shared across tests | 1 | **Harness artifact** of one-database pinning |

---

## What works

**The sink connector is solid.** `MongoSinkConnectorIntegrationTest` passes 7/7 — writes, regex
topic mapping, restart survival, and multi-task/multi-partition fan-out all behave as on native
MongoDB. Of the 11 `MongoSinkTaskIntegrationTest` tests, the 3 that do not touch time series or
depend on read-back ordering also pass.

**Change streams work at both database and collection scope**, the latter once the collection-group
stream is provisioned. Events are correctly shaped: `operationType`, `documentKey`, `fullDocument`,
`ns`, resume tokens and heartbeats all behave. Resume tokens round trip through the connector, but
their `_data` is not MongoDB's encoding and command replies omit `operationTime` — see §6.

**`startup.mode=timestamp` is honoured.** A stream configured with a past `startAtOperationTime`
replays retained history and delivers the pre-start insert with the correct `fullDocument`;
`testStartAtOperationTime` passes once the harness stops asking it to replay from the epoch. The
same run shows Firestore emits `invalidate` on collection drop with MongoDB's shape.

**End-to-end round trips work.** `ChangeStreamRoundTripTest` replicates a whole database and a whole
collection through Kafka into a second Firestore endpoint and compares equal.
`FullDocumentRoundTripIntegrationTest` passes 3/5 — the default, simplified-JSON and BSON output
formats all round trip 99 documents faithfully, covering every BSON type the test exercises
(`ObjectId`, `Binary`, `Date`, `Decimal128`, nested documents, arrays).

**Heartbeats, batch sizing and `errors.tolerance=none` all pass** now that their collection-scope
streams exist.

**Connector configuration validation works** for the 8 cases that do not need a provisioned user or
a time series collection.

**Firestore presents as a sharded cluster.** `isMaster` returns `msg: isdbgrid`, so the connector's
`isReplicaSetOrSharded()` precondition for change streams is satisfied without adaptation.

---

## What does not

### 1. Ordering is not insertion order — 19 tests

By far the largest single source of failures, and the one finding that matters most. Inserting `_id`
1…20 in order and reading back with `aggregate([])` returns them shuffled:

```
16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13
```

Native MongoDB returns natural (insertion) order for a collection with no deletes. The **set** of
documents is always correct — only the order differs.

This hits three places.

**Copy-existing snapshots — 13 tests.** `MongoCopyDataManager.copyDataFrom()` runs its snapshot
`aggregate` with no `$sort`
(`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`), so
`startup.mode=copy_existing` emits the existing documents in arbitrary order. This is what fails the
copy-existing, restart-mid-copy, document-only and topic-mapping tests.

**Unsorted read-backs — 4 tests.** The four `MongoSinkTaskIntegrationTest` error-tolerance tests
read the target collection back with a plain `find()` and compare element by element. The connector
is not implicated at all here; the test's own read is unsorted.

**Live change stream events — 2 tests.** This is the part that is *not* just a snapshot artifact.
`testDeadletterQueueHandling` and `testSourceCanUseCustomOffsetPartitionNames` both call
`task.start(...)` first and only then write, so every record they assert on comes from the live
stream. Each writes with a single ordered `insertMany` (`MongoKafkaTestCase.java:403`) and gets the
events back in a different order:

```
testDeadletterQueueHandling            insertMany(_id 1..5)   ->  index [0] expected {"_id": 1}, was {"_id": 5}
testSourceCanUseCustomOffsetPartitionNames  insertMany(_id 1..50)  ->  index [0] expected Insert{id=1}, was Insert{id=9}
```

So Firestore does not preserve write order *within a batch* when emitting change events. MongoDB
delivers a change stream in commit order, and an ordered `insertMany` commits in list order.

Snapshot ordering is not a documented guarantee of copy-existing, so that part is usable for
consumers that do not depend on it. The live-stream part is the more consequential finding: a
consumer that relies on change events arriving in the order the writes were issued — an ordered
replication target, or anything that folds events into a running state per key — will see a
different order from Firestore. Per-document ordering is not contradicted by anything here; only
relative ordering across documents written in one batch is.

### 2. Time series collections are unsupported, and expose a connector NPE — 7 tests

`createCollection` rejects the option outright:

```
Unsupported fields in createCollection request: [timeseries]
```

Separately, Firestore's `collStats` reply omits `nindexes` — it returns
`{ok, ns, count, storageSize, inlineScanEligibleFields}`. `TimeseriesValidation.java:249` reads
`collStats.getInteger("nindexes") > 0` unguarded, so it throws a `NullPointerException` instead of
producing the intended validation error:

```
java.lang.NullPointerException: Cannot invoke "java.lang.Integer.intValue()" because the
return value of "org.bson.Document.getInteger(Object)" is null
```

This is as much a connector robustness bug as a Firestore gap — `nindexes` is not guaranteed by the
`collStats` contract, and a null-safe read would surface the real error. It is **not fixed here**:
`src/main/` is out of scope. Fixing it would change the failure message, not the outcome, since
time series collections cannot be created either way.

### 3. Error codes differ, and the connector's recovery paths are keyed on them — 2 tests

Firestore returns its own error codes for conditions MongoDB has dedicated codes for. The connector
matches on the MongoDB codes, so two of its documented behaviours silently stop working.

* **Stale resume token.** A token the server will not accept comes back as
  `Internal error encountered. 1`, not `260` (`ChangeStreamFatalError`) or `286`
  (`ChangeStreamHistoryLost`). `StartedMongoSourceTask` keys its `errors.tolerance=all` recovery on
  those codes (`INVALID_CHANGE_STREAM_ERRORS`), so it never recognises the token as invalid, retries
  with the same token, logs `Unable to recreate the cursor` and leaves the cursor null. Every
  subsequent `poll()` returns nothing. **The connector's documented recovery from a bad offset does
  not work against Firestore.** The other two workarounds it prints — a new `offset.partition.name`,
  or removing the offset from storage — still apply.
* **Disallowed pipeline stage.** `[{$group: {_id: 1}}]` in `pipeline` is refused, which matches
  MongoDB — but as `Stage GROUP is not allowed in change stream 2` (code `2`, `InvalidArgument`)
  rather than code `20` (`IllegalOperation`). The connector's `Illegal $changeStream operation`
  guidance is keyed on code `20`, so it never fires; the task treats the rejection as a resume
  failure and retried 140 times in this run. A misconfigured pipeline presents as a silently stalled
  connector rather than a clear error.

### 4. Change stream payload and option gaps — 3 tests

* **`changeStreamPreAndPostImages`** is not an accepted `createCollection` option
  (`Unsupported fields in createCollection request: [changeStreamPreAndPostImages]`), so
  `change.stream.full.document.before.change` cannot be exercised and `fullDocumentBeforeChange` is
  never populated.
* **`showExpandedEvents`** is not an accepted change stream option. The server rejects the
  aggregate with `invalid field(s) in change stream: [showExpandedEvents] 2`. The connector logs
  this as a resume failure and retries rather than surfacing it, so the test observes an empty poll
  rather than an error. `disambiguatedPaths` is carried by the expanded event, so it is untested
  rather than shown to be wrong.
* **`updateDescription.truncatedArrays`** is not populated. Shortening an array from 11 to 10
  elements yields an update event whose `truncatedArrays` is `null` where MongoDB reports
  `[{field: items, newSize: 10}]`. The event arrives and `operationType` is correct — only this one
  field of the payload differs.

The two `disambiguatedPaths` tests that assert the field is **absent** both pass, which confirms
the default event shape is correct and isolates the gap to the expanded-events option.

### 5. Document size limit is below MongoDB's — 1 test

The document written to force an oversized change event is refused by the write itself:

```
WriteError{code=2, message='entity is too big'}
```

Firestore's per-entity limit sits well below MongoDB's 16 MB BSON limit. The finding is the write
limit, which applies to the sink connector too; what is left untested is the connector's
`errors.tolerance=all` handling of a change event over 16 MB (`BSON_OBJECT_TOO_LARGE`, error
`10334`), because the size that would trigger it is unreachable here.

### 6. Command replies omit `operationTime`, and resume tokens are not MongoDB's format — 0 tests

**No test fails on this**, which is exactly why it is worth recording: nothing in the suite asserts
on it, so it would otherwise go unreported. It fires roughly **310 times per sweep**
(`fs-results8`: 310 — `FullDocumentRoundTripIntegrationTest` 94, `MongoSourceConnectorIntegrationTest`
84, `MongoSourceTaskIntegrationTest` 68, `ChangeStreamRoundTripTest` 64):

```
WARN  Exception thrown raising command succeeded event to listener
java.lang.NullPointerException: Cannot invoke "org.bson.BsonValue.asTimestamp()" because the
return value of "org.bson.BsonDocument.get(Object)" is null
  at com.mongodb.kafka.connect.util.ResumeTokenUtils.lambda$getResponseOffsetSecs$2(ResumeTokenUtils.java:107)
  at com.mongodb.kafka.connect.source.MongoSourceTask.mongoCommandSucceeded(MongoSourceTask.java:250)
```

`ResumeTokenUtils.getResponseOffsetSecs` computes `response.operationTime` minus the timestamp
inside `cursor.postBatchResumeToken`. Reaching line 107 proves `cursor.postBatchResumeToken` was
present, so the null is **`operationTime`**: Firestore does not return it on aggregate/getMore
replies.

There is a second difference behind it that the NPE currently masks. Firestore's `_data` is base64
protobuf, not MongoDB's hex encoding. `parseHex` on a real token gives
`digit('C')=12, digit('l')=-1` → byte `0xFF` → `canonicalType = 255`, and line 76 requires `130`, so
`getTimestampFromResumeToken` would throw `IllegalArgumentException` too. It never gets the chance —
Java evaluates line 107's left operand first, and `"canonical type"` appears in no run log.

The cost is contained but real: the two `sample()` calls above it already ran, so the only casualty
is **`latest-mongodb-time-difference-secs`**, the connector's replication-lag gauge, which is
permanently unpopulated against Firestore — plus the log noise. It is swallowed by the driver's
command-listener guard, so no test result is affected.

This is also a connector robustness bug in its own right: a best-effort metrics helper should not
throw out of a `CommandListener`, and `operationTime` is unguarded where the two fields before it in
the same chain are. `parseHex` compounds it by ORing in `Character.digit`'s `-1` for a non-hex
character rather than rejecting, so non-hex input yields silent garbage and an error that names the
wrong thing. **Not fixed here:** `src/main/` is out of scope, and patching it would erase the
signal.

---

## Failures that are not Firestore's

Reported as FAIL because that is what the run produced, but they carry no compatibility signal. The
second entry is a **0-test** record of a failure mode that earlier sweeps hit and this one does not;
it is kept because the conditions that cause it are still present.

* **Schema Registry subject shared across tests — 1 test.**
  `FullDocumentRoundTripIntegrationTest.testRoundTripSchema` dies on
  `Schema being registered is incompatible with an earlier schema for subject
  "copy.changestream.source-value"`. Upstream, each test in the class gets its own
  `getDatabaseWithPostfix()` database and therefore its own topic and subject; Firestore cannot
  create databases on demand, so all five collapse onto one topic and this test inherits the schema
  an earlier test registered. Recoverable by giving each test a distinct *collection* name — the
  trick `getAndCreateCollection()` already uses when the database is pinned — at the cost of one
  provisioned change stream per test. Not done here, because it would turn a red test green.
* **Change stream history retained across runs — 0 tests in this sweep; recorded because it was a
  failure in `fs-results6` and `fs-results7` and the failure mode can return.** As of `fs-results8`
  `testStartAtOperationTime` **passes**. Previously it asked for `startAtOperationTime` at the epoch
  and got nothing.
  Firestore clamps the epoch to the oldest event it still retains for the collection group, and
  under one-database pinning the group `coll` is shared by six tests with 24h retention **spanning
  runs**. The first event replayed was an `invalidate` with `clusterTime 2026-08-18T12:08:22.552Z` —
  90 minutes before the run, left by the previous sweep's teardown drop of `coll`. An `invalidate`
  closes the cursor; the connector reinitialises at the same configured operation time, reads the
  same `invalidate`, and discards it because an `invalidate` carries no `ns` and maps to no topic
  (`No topic set. Could not publish the message`). Zero records against an expected 1, task dead
  after 355 ms, the test's own insert never reached. Upstream this cannot happen — each test gets a
  fresh database, so the replay window holds only its own writes.

  **The fix, in place for this run.** The test now takes its start time from the clock after its own
  drop instead of from the epoch. That is the harness's one declared deviation
  (see the harness documentation, §"Declared deviation"), because unlike every other adaptation it
  changes a value the connector consumes. What the fixed test then establishes is a positive result: **Firestore honours
  `startAtOperationTime`** and replays retained history — it delivered a 92-minute-old event — and
  it emits `invalidate` on collection drop with the same shape as MongoDB.

---

## What is left untested

### User and role provisioning — 9 tests

`createUser` and `createRole` are unsupported commands; Firestore administers users through Google
Cloud, not the wire protocol. `runUserManagementCommand()` converts *only* that specific failure
into a skip, with the server's own error attached, so this is self-detecting — on native MongoDB
the command succeeds and nothing is skipped. The connector's auth/permission validation is therefore
untested here. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and
supplying their credentials would recover it.

### Cross-database fan-in — 5 tests, out of scope

These test one connector watching the whole deployment and fanning in changes from three databases
at once. A Firestore instance holds a single database, so this behaviour does not exist to be
tested — it is out of scope rather than unsupported, and no harness configuration recovers it.

### Not run at all

* `MongoSourceTaskIntegrationTest2` — a Mockito unit test stranded in the `integrationTest` source
  set. Never reaches a server, so it carries no compatibility signal.
* `sink.CsfleIntegrationTest` — requires `crypt_shared` or `mongocryptd`, neither installed. All
  four tests self-skip, leaving CSFLE-over-Firestore untested rather than shown to be unsupported.

---

## Behaviours confirmed by direct probe

Verified with mongosh against the source endpoint, independent of the test suite:

| Scope | Call | Result |
|---|---|---|
| Deployment | `client.watch()` | ``unsupported database `admin` `` |
| Database | `db.watch()` | OK |
| Collection, unprovisioned | `db.getCollection("source").watch()` | `code 2 (InvalidArgument)` — stream not active |
| Collection, provisioned | `db.getCollection("source").watch()` | OK |

`dropDatabase` is also unsupported (`code 2 / InvalidArgument`), which is why every teardown path in
the harness drops collections individually instead. The `db.drop()` calls that sit in *test bodies*
are deliberately left in place — those tests assert on the resulting change event, so the drop is
test logic, and their failure is a genuine signal.

---

<!-- BEGIN GENERATED PER-TEST DETAIL -->

> Generated by `generate-report.py` from the archived JUnit XML. Edits here are
> overwritten — change the script or the narrative sections above instead.

## `com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest`

24 tests — 6 PASS, 16 FAIL, 2 BLOCKED. Results from `fs-results8/`.

### Test: `MongoSourceTaskIntegrationTest.testCopyingExistingWithARestartAfterFinishing`

* **Display name:** Copy existing with a restart after finishing
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=25}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1266)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testCopyingExistingWithARestartAfterFinishing(MongoSourceTaskIntegrationTest.java:652)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testCopyingExistingWithARestartMidwayThrough`

* **Display name:** Copy existing with a restart midway through
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=25}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1266)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testCopyingExistingWithARestartMidwayThrough(MongoSourceTaskIntegrationTest.java:579)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testDeadletterQueueHandling`

* **Display name:** Ensure source sends data to the deadletter queue on failures
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <{"_id": 1}> but was: <{"_id": 5}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testDeadletterQueueHandling(MongoSourceTaskIntegrationTest.java:870)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testDisambiguatedPathsExistWhenShowExpandedEventsIsTrue`

* **Display name:** Ensure disambiguatedPaths exist when showExpandedEvents is true
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** `showExpandedEvents` is not an accepted change stream option. The connector adds it to the `$changeStream` stage when `change.stream.show.expanded.events=true`, and the server rejects the aggregate with `invalid field(s) in change stream: [showExpandedEvents] 2`. The stream never opens, so the task polls zero records against an expected 3.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: expected: <3> but was: <0>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testDisambiguatedPathsExistWhenShowExpandedEventsIsTrue(MongoSourceTaskIntegrationTest.java:1089)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/change-streams. The connector logs this as a resume failure (`StartedMongoSourceTask.java:471`) and keeps retrying rather than surfacing it, so the test sees an empty poll rather than an error. `disambiguatedPaths` itself is therefore **untested** — it is carried by the expanded event the option enables.

### Test: `MongoSourceTaskIntegrationTest.testErrorToleranceAllSupport`

* **Display name:** Ensure source honours error tolerance all
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    junit.framework.AssertionFailedError: Struct differed in position [0] : Field: '_id' differs ==> expected: <1> but was: <2>
    at com.mongodb.kafka.connect.source.schema.SchemaUtils.assertStructsEquals(SchemaUtils.java:47)
    at com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testErrorToleranceAllSupport(MongoSourceTaskIntegrationTest.java:912)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testErrorToleranceAllSupport16MbError`

* **Display name:** Ensure source honours error tolerance all and > 16mb change stream message
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** The document the test writes to force an oversized change event is refused by the write itself: `WriteError{code=2, message='entity is too big'}`. Firestore's per-entity limit is well below MongoDB's 16 MB BSON limit, so the test cannot even construct the precondition it needs.
  * **Raw Error / Assertion Mismatch:**

    ```
    com.mongodb.MongoWriteException: Write operation error on MongoDB server <endpoint>. Write error: WriteError{code=2, message='entity is too big', details={}}.
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testErrorToleranceAllSupport16MbError(MongoSourceTaskIntegrationTest.java:997)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. What is left **untested** is the connector's `errors.tolerance=all` handling of a change event over 16 MB (`BSON_OBJECT_TOO_LARGE`, error `10334`); the size limit that would trigger it is unreachable here. The relevant finding is the write limit itself, which applies to the sink connector too.

### Test: `MongoSourceTaskIntegrationTest.testFullDocumentBeforeChange`

* **Display name:** Ensure pre-/post-image works
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** `changeStreamPreAndPostImages` is not an accepted `createCollection` option, so pre-/post-images cannot be enabled and `fullDocumentBeforeChange` is never populated.
  * **Raw Error / Assertion Mismatch:**

    ```
    com.mongodb.MongoCommandException: Command execution failed on MongoDB server with error 2 (InvalidArgument): 'Unsupported fields in createCollection request: [changeStreamPreAndPostImages]' on server <endpoint>. The full response is {"ok": 0.0, "errmsg": "Unsupported fields in createCollection request: [changeStreamPreAndPostImages]", "code": 2, "codeName": "InvalidArgument"}
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testFullDocumentBeforeChange(MongoSourceTaskIntegrationTest.java:1036)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/change-streams. Affects `change.stream.full.document.before.change`.

### Test: `MongoSourceTaskIntegrationTest.testSourceCanHandleInvalidResumeTokenWhenErrorToleranceIsAll`

* **Display name:** Ensure source can handle invalid resume token when error tolerance is set to all
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** A resume token the server will not accept is reported as `Internal error encountered. 1`, not as MongoDB's `260` (`ChangeStreamFatalError` / invalidated resume token) or `286` (`ChangeStreamHistoryLost`). `StartedMongoSourceTask` keys its recovery on those codes (`INVALID_CHANGE_STREAM_ERRORS`, `StartedMongoSourceTask.java:104`), so with `errors.tolerance=all` it never recognises the token as invalid, retries with the same token three times, logs `Unable to recreate the cursor` and leaves the cursor null. Every subsequent `poll()` returns nothing, so the 50 inserts that follow are never emitted.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable lengths differ, expected: <50> but was: <0>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1266)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testSourceCanHandleInvalidResumeTokenWhenErrorToleranceIsAll(MongoSourceTaskIntegrationTest.java:427)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. The connector's documented recovery from a stale offset (`errors.tolerance=all`) does not work against Firestore. The workarounds the connector prints alongside the error — a new `offset.partition.name`, or removing the offset from its storage — still apply, because they avoid presenting the bad token in the first place.

### Test: `MongoSourceTaskIntegrationTest.testSourceCanHandleNonExistentCollectionAndSurviveDropping`

* **Display name:** Ensure source can handle non existent collection and survive dropping
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
  * the in-test-body `db.drop()` is **deliberately left in place** — see above
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=2}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1266)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testSourceCanHandleNonExistentCollectionAndSurviveDropping(MongoSourceTaskIntegrationTest.java:385)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testSourceCanHandleNonExistentDatabaseAndSurviveDropping`

* **Display name:** Ensure source can handle non existent database and survive dropping
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
  * the in-test-body `db.drop()` is **deliberately left in place** — this test exercises change stream behaviour across a database drop, so the drop is test logic, not teardown
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.gradle.internal.exceptions.DefaultMultiCauseException: Multiple Failures (3 failures)
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=4}>
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=8}>
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=101}> but was: <Insert{id=102}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testSourceCanHandleNonExistentDatabaseAndSurviveDropping(MongoSourceTaskIntegrationTest.java:286)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1266)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.lambda$testSourceCanHandleNonExistentDatabaseAndSurviveDropping$23(MongoSourceTaskIntegrationTest.java:287)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testSourceCanHandleNonExistentDatabaseAndSurviveDroppingWithPipeline`

* **Display name:** Ensure source can handle non existent database and survive dropping with pipeline
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
  * the in-test-body `db.drop()` is **deliberately left in place** — see above
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.gradle.internal.exceptions.DefaultMultiCauseException: Multiple Failures (3 failures)
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=4}>
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=8}>
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=101}> but was: <Insert{id=102}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testSourceCanHandleNonExistentDatabaseAndSurviveDroppingWithPipeline(MongoSourceTaskIntegrationTest.java:342)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1266)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.lambda$testSourceCanHandleNonExistentDatabaseAndSurviveDroppingWithPipeline$33(MongoSourceTaskIntegrationTest.java:343)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testSourceCanUseCustomOffsetPartitionNames`

* **Display name:** Ensure source can use custom offset partition names
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
  * up-front `skipIfCollectionScopeChangeStream("coll")` — the shared check runs inside `assertDoesNotThrow(...)` here, which would turn the abort into a FAILED rather than a SKIPPED
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=9}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1266)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testSourceCanUseCustomOffsetPartitionNames(MongoSourceTaskIntegrationTest.java:544)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testSourceEmitsNullValuesOnDelete`

* **Display name:** Test null values are emitted when documents are deleted
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Document{{myInt=11, _id=6a856206490efada1ac298a4}}> but was: <Document{{myInt=58, _id=6a856206490efada1ac298d3}}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testSourceEmitsNullValuesOnDelete(MongoSourceTaskIntegrationTest.java:779)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testSourceLoadsDataFromCollectionDocumentOnly`

* **Display name:** Ensure source loads data from collection and outputs documents only
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Document{{myInt=11, myString=some foo bla text, myDouble=20.21, mySubDoc=Document{{A=S2Fma2Egcm9ja3Mh, B=2020-01-01T07:27:07Z, C=12345.6789}}, myArray=[S2Fma2Egcm9ja3Mh, 2020-01-01T07:27:07Z, 12345.6789], myBytes=S2Fma2Egcm9ja3Mh, myDate=2020-01-01T07:27:07Z, myDecimal=12345.6789, _id=6a855f0b490efada1ac29859}}> but was: <Document{{myInt=57, myString=some foo bla text, myDouble=20.21, mySubDoc=Document{{A=S2Fma2Egcm9ja3Mh, B=2020-01-01T07:27:07Z, C=12345.6789}}, myArray=[S2Fma2Egcm9ja3Mh, 2020-01-01T07:27:07Z, 12345.6789], myBytes=S2Fma2Egcm9ja3Mh, myDate=2020-01-01T07:27:07Z, myDecimal=12345.6789, _id=6a855f0b490efada1ac29887}}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testSourceLoadsDataFromCollectionDocumentOnly(MongoSourceTaskIntegrationTest.java:740)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testSourceTopicMapping`

* **Display name:** Ensure source sets the expected topic mapping
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.gradle.internal.exceptions.DefaultMultiCauseException: Multiple Failures (2 failures)
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=25}>
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=25}>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testSourceTopicMapping(MongoSourceTaskIntegrationTest.java:476)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.lambda$testSourceTopicMapping$39(MongoSourceTaskIntegrationTest.java:477)
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.assertSourceRecordValues(MongoSourceTaskIntegrationTest.java:1287)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceTaskIntegrationTest.testTruncatedArrays`

* **Display name:** Ensure truncatedArrays works
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * **Failure Reason:** `updateDescription.truncatedArrays` is not populated. An update that shortens an array from 11 to 10 elements produces an update event whose `truncatedArrays` is `null` where MongoDB reports `[{field: items, newSize: 10}]`. The event itself arrives and `operationType` is correct — only this field of the payload differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: expected: <[Struct{field=items,newSize=10}]> but was: <null>
    at app//com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest.testTruncatedArrays(MongoSourceTaskIntegrationTest.java:1203)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. A payload-level difference, not a scope or provisioning gap: the change stream opened and delivered all three events. Consumers that read `truncatedArrays` to reconstruct array mutations will not get it from Firestore.

### Test: `MongoSourceTaskIntegrationTest.testSourceLoadsDataFromMongoClient`

* **Display name:** Ensure source loads data from MongoClient
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test needs two or more *distinct* databases. Firestore cannot create a database on demand (`Invalid database name: changestream999`), so `org.mongodb.test.database` pins every ad-hoc database to the one that exists. The namespaces would collapse into one and the test would fail on `code 11000 Document already exists` — a harness artifact, not a Firestore signal — so `getDatabaseWithPostfix()` aborts on the second handout instead.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. These tests exist to prove that one connector watching the whole deployment fans in changes from several databases at once. A Firestore instance holds a single database, so that behaviour does not exist to be tested here — this is **out of scope rather than unsupported**, and no harness configuration recovers it.

### Test: `MongoSourceTaskIntegrationTest.testSourceLoadsDataFromMongoClientWithCopyExisting`

* **Display name:** Ensure source loads data from MongoClient with copy existing data
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test needs two or more *distinct* databases. Firestore cannot create a database on demand (`Invalid database name: changestream999`), so `org.mongodb.test.database` pins every ad-hoc database to the one that exists. The namespaces would collapse into one and the test would fail on `code 11000 Document already exists` — a harness artifact, not a Firestore signal — so `getDatabaseWithPostfix()` aborts on the second handout instead.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. These tests exist to prove that one connector watching the whole deployment fans in changes from several databases at once. A Firestore instance holds a single database, so that behaviour does not exist to be tested here — this is **out of scope rather than unsupported**, and no harness configuration recovers it.

### Test: `MongoSourceTaskIntegrationTest.testDisambiguatedPathsDontExistByDefault`

* **Display name:** Ensure disambiguatedPaths don't exist by default
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (48.6s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSourceTaskIntegrationTest.testDisambiguatedPathsDontExistWhenShowExpandedEventsIsTrue`

* **Display name:** Ensure disambiguatedPaths don't exist when showExpandedEvents is false
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (48.8s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSourceTaskIntegrationTest.testErrorToleranceNoneSupport`

* **Display name:** Ensure source honours error tolerance none
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (44.0s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSourceTaskIntegrationTest.testHonoursMaxBatchSize`

* **Display name:** Ensure source honours poll max batch size and batch size
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (72.9s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSourceTaskIntegrationTest.testSourceGeneratesHeartbeats`

* **Display name:** Ensure source generates heartbeats
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (51.0s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSourceTaskIntegrationTest.testStartAtOperationTime`

* **Display name:** testStartAtOperationTime()
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset, so it watches at database scope rather than the deployment scope Firestore does not have; a test that sets `database` itself is unaffected
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (81.7s). No behaviour difference observed.
* **Notes / Docs Reference:** —

## `com.mongodb.kafka.connect.MongoSourceConnectorIntegrationTest`

9 tests — 2 PASS, 4 FAIL, 3 BLOCKED. Results from `fs-results8/`.

### Test: `MongoSourceConnectorIntegrationTest.testSchemaKeyAndValueOutput`

* **Display name:** Ensure Schema Key and Value output
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <2> but was: <1>
    at app//com.mongodb.kafka.connect.MongoSourceConnectorIntegrationTest.testSchemaKeyAndValueOutput(MongoSourceConnectorIntegrationTest.java:335)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceConnectorIntegrationTest.testSourceHasFriendlyErrorMessagesForInvalidPipelines`

* **Display name:** Ensure Source provides friendly error messages for invalid pipelines
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * **Failure Reason:** A disallowed aggregation stage in `pipeline` is rejected as `Stage GROUP is not allowed in change stream 2` (error code `2`, `InvalidArgument`), where MongoDB returns code `20` (`IllegalOperation`). The connector matches on code `20` (`ILLEGAL_OPERATION_ERROR`, `StartedMongoSourceTask.java:98`) to raise its `Illegal $changeStream operation` guidance, so that path never fires: the task treats the rejection as a resume failure and retries (140 times in this run) instead of failing fast with the friendly message the test asserts on.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: expected: <true> but was: <false>
    at app//com.mongodb.kafka.connect.MongoSourceConnectorIntegrationTest.testSourceHasFriendlyErrorMessagesForInvalidPipelines(MongoSourceConnectorIntegrationTest.java:430)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/change-streams. The stage really is refused — that part matches MongoDB. Only the error code differs, and the connector's diagnostics are keyed on the code. A misconfigured pipeline therefore presents as a silently stalled connector rather than a clear error.

### Test: `MongoSourceConnectorIntegrationTest.testSourceLoadsDataFromCollectionCopyExistingBson`

* **Display name:** Ensure source loads data from collection with copy existing data - outputting bson
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=25}>
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.assertProduced(MongoKafkaTestCase.java:535)
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.assertProduced(MongoKafkaTestCase.java:493)
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.assertProduced(MongoKafkaTestCase.java:481)
    at app//com.mongodb.kafka.connect.MongoSourceConnectorIntegrationTest.testSourceLoadsDataFromCollectionCopyExistingBson(MongoSourceConnectorIntegrationTest.java:240)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceConnectorIntegrationTest.testSourceLoadsDataFromCollectionCopyExistingJson`

* **Display name:** Ensure source loads data from collection with copy existing data - outputting json
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Insert{id=1}> but was: <Insert{id=25}>
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.assertProduced(MongoKafkaTestCase.java:535)
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.assertProduced(MongoKafkaTestCase.java:493)
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.assertProduced(MongoKafkaTestCase.java:470)
    at app//com.mongodb.kafka.connect.MongoSourceConnectorIntegrationTest.testSourceLoadsDataFromCollectionCopyExistingJson(MongoSourceConnectorIntegrationTest.java:213)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSourceConnectorIntegrationTest.testSourceLoadsDataFromCollectionCopyExistingByRegex`

* **Display name:** Ensure source loads data from collection with copy existing data by regex
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test needs two or more *distinct* databases. Firestore cannot create a database on demand (`Invalid database name: changestream999`), so `org.mongodb.test.database` pins every ad-hoc database to the one that exists. The namespaces would collapse into one and the test would fail on `code 11000 Document already exists` — a harness artifact, not a Firestore signal — so `getDatabaseWithPostfix()` aborts on the second handout instead.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. These tests exist to prove that one connector watching the whole deployment fans in changes from several databases at once. A Firestore instance holds a single database, so that behaviour does not exist to be tested here — this is **out of scope rather than unsupported**, and no harness configuration recovers it.

### Test: `MongoSourceConnectorIntegrationTest.testSourceLoadsDataFromMongoClient`

* **Display name:** Ensure source loads data from MongoClient
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test needs two or more *distinct* databases. Firestore cannot create a database on demand (`Invalid database name: changestream999`), so `org.mongodb.test.database` pins every ad-hoc database to the one that exists. The namespaces would collapse into one and the test would fail on `code 11000 Document already exists` — a harness artifact, not a Firestore signal — so `getDatabaseWithPostfix()` aborts on the second handout instead.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. These tests exist to prove that one connector watching the whole deployment fans in changes from several databases at once. A Firestore instance holds a single database, so that behaviour does not exist to be tested here — this is **out of scope rather than unsupported**, and no harness configuration recovers it.

### Test: `MongoSourceConnectorIntegrationTest.testSourceLoadsDataFromMongoClientWithCopyExisting`

* **Display name:** Ensure source loads data from MongoClient with copy existing data
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test needs two or more *distinct* databases. Firestore cannot create a database on demand (`Invalid database name: changestream999`), so `org.mongodb.test.database` pins every ad-hoc database to the one that exists. The namespaces would collapse into one and the test would fail on `code 11000 Document already exists` — a harness artifact, not a Firestore signal — so `getDatabaseWithPostfix()` aborts on the second handout instead.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. These tests exist to prove that one connector watching the whole deployment fans in changes from several databases at once. A Firestore instance holds a single database, so that behaviour does not exist to be tested here — this is **out of scope rather than unsupported**, and no harness configuration recovers it.

### Test: `MongoSourceConnectorIntegrationTest.testSourceHeartbeatsHaveValidSchema`

* **Display name:** Ensure Source heartbeats have a valid schema
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (11.5s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSourceConnectorIntegrationTest.testSourceUsesHeartbeatsForOffsets`

* **Display name:** Ensure Source uses heartbeats for creating offsets
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
  * `org.mongodb.test.force.database.scope` supplies a source database to a connector that left one unset (see above)
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (108.5s). No behaviour difference observed.
* **Notes / Docs Reference:** —

## `com.mongodb.kafka.connect.MongoSinkConnectorIntegrationTest`

7 tests — 7 PASS, 0 FAIL, 0 BLOCKED. Results from `fs-results8/`.

### Test: `MongoSinkConnectorIntegrationTest.testSinkSavesAvroDataToMongoDB`

* **Display name:** Ensure sink connect saves data to MongoDB
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (48.5s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSinkConnectorIntegrationTest.testSinkSavesAvroDataToMongoDBWhenUsingRegex`

* **Display name:** Ensure sink connect saves data to MongoDB when using regex
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (90.8s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSinkConnectorIntegrationTest.testSinkSavesToMultipleCollectionsUsingMultipleTasksWithMultiplePartitions`

* **Display name:** Ensure sink saves data to multiple collections using multiple tasks and multiple partitions
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (90.7s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSinkConnectorIntegrationTest.testSinkSavesUsingASingleTasksWithMultiplePartitions`

* **Display name:** Ensure sink saves data using a single task and multiple partitions
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (48.0s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSinkConnectorIntegrationTest.testSinkSavesUsingMultipleTasksWithASinglePartition`

* **Display name:** Ensure sink saves data using multiple tasks and a single partition
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (49.4s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSinkConnectorIntegrationTest.testSinkSavesUsingMultipleTasksWithMultiplePartitions`

* **Display name:** Ensure sink saves data using multiple tasks and multiple partitions
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (48.5s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSinkConnectorIntegrationTest.testSinkSurvivesARestart`

* **Display name:** Ensure sink can survive a restart
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (48.5s). No behaviour difference observed.
* **Notes / Docs Reference:** —

## `com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest`

11 tests — 3 PASS, 8 FAIL, 0 BLOCKED. Results from `fs-results8/`.

### Test: `MongoSinkTaskIntegrationTest.testSinkCanHandleInvalidCDCWhenErrorToleranceIsAll`

* **Display name:** Ensure sink can handle poison pill CDC value
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Document{{_id=1, a=1, b=2}}> but was: <Document{{_id=2, a=1, b=2}}>
    at app//com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest.testSinkCanHandleInvalidCDCWhenErrorToleranceIsAll(MongoSinkTaskIntegrationTest.java:374)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSinkTaskIntegrationTest.testSinkCanHandleInvalidDocumentWhenErrorToleranceIsAll`

* **Display name:** Ensure sink can handle poison pill invalid document
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Document{{_id=1, a=a1, b=b}}> but was: <Document{{_id=2, a=a2, b=b}}>
    at app//com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest.testSinkCanHandleInvalidDocumentWhenErrorToleranceIsAll(MongoSinkTaskIntegrationTest.java:331)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSinkTaskIntegrationTest.testSinkCanHandleInvalidKeyWhenErrorToleranceIsAll`

* **Display name:** Ensure sink can handle poison pill invalid key
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [1], expected: <Document{{a=a2, b=b, c=2}}> but was: <Document{{a=a6, b=b, c=6}}>
    at app//com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest.testSinkCanHandleInvalidKeyWhenErrorToleranceIsAll(MongoSinkTaskIntegrationTest.java:229)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSinkTaskIntegrationTest.testSinkCanHandleInvalidValueWhenErrorToleranceIsAll`

* **Display name:** Ensure sink can handle poison pill invalid value
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [0], expected: <Document{{a=a1, b=b, c=1}}> but was: <Document{{a=a2, b=b, c=2}}>
    at app//com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest.testSinkCanHandleInvalidValueWhenErrorToleranceIsAll(MongoSinkTaskIntegrationTest.java:286)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `MongoSinkTaskIntegrationTest.testSinkProcessesTimeseriesData`

* **Display name:** Ensure sink processes timeseries data from Kafka
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** `collStats` omits `nindexes`. Firestore's reply is `{ok, ns, count, storageSize, inlineScanEligibleFields}`; `TimeseriesValidation.java:249` reads `collStats.getInteger("nindexes") > 0` unguarded and throws NPE.
  * **Raw Error / Assertion Mismatch:**

    ```
    java.lang.NullPointerException: Cannot invoke "java.lang.Integer.intValue()" because the return value of "org.bson.Document.getInteger(Object)" is null
    at com.mongodb.kafka.connect.util.TimeseriesValidation.shouldCreateCollection(TimeseriesValidation.java:249)
    at com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection(TimeseriesValidation.java:118)
    at com.mongodb.kafka.connect.sink.StartedMongoSinkTask.checkTimeseries(StartedMongoSinkTask.java:187)
    at com.mongodb.kafka.connect.sink.StartedMongoSinkTask.bulkWriteBatch(StartedMongoSinkTask.java:143)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. This is as much a connector robustness bug as a Firestore gap — `nindexes` is not guaranteed by the `collStats` contract, and a null-safe read would produce the intended validation error instead of an NPE. Time series collections are separately unsupported (`Unsupported fields in createCollection request: [timeseries]`), so this test could not pass even with `nindexes` present. Not fixed: `src/main/` is out of scope.

### Test: `MongoSinkTaskIntegrationTest.testSinkRegexTimeseriesCannotCreate`

* **Display name:** Ensure sink regex timeseries errors if cannot create
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** `collStats` omits `nindexes`. Firestore's reply is `{ok, ns, count, storageSize, inlineScanEligibleFields}`; `TimeseriesValidation.java:249` reads `collStats.getInteger("nindexes") > 0` unguarded and throws NPE.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: Unexpected exception type thrown ==> expected: <org.apache.kafka.common.config.ConfigException> but was: <java.lang.NullPointerException>
    at app//com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest.testSinkRegexTimeseriesCannotCreate(MongoSinkTaskIntegrationTest.java:412)
    at com.mongodb.kafka.connect.util.TimeseriesValidation.shouldCreateCollection(TimeseriesValidation.java:249)
    at com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection(TimeseriesValidation.java:118)
    at com.mongodb.kafka.connect.sink.StartedMongoSinkTask.checkTimeseries(StartedMongoSinkTask.java:187)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. This is as much a connector robustness bug as a Firestore gap — `nindexes` is not guaranteed by the `collStats` contract, and a null-safe read would produce the intended validation error instead of an NPE. Time series collections are separately unsupported (`Unsupported fields in createCollection request: [timeseries]`), so this test could not pass even with `nindexes` present. Not fixed: `src/main/` is out of scope.

### Test: `MongoSinkTaskIntegrationTest.testSinkRegexTimeseriesMissingTimefield`

* **Display name:** Ensure sink regex timeseries errors missing timefield create
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** `collStats` omits `nindexes`. Firestore's reply is `{ok, ns, count, storageSize, inlineScanEligibleFields}`; `TimeseriesValidation.java:249` reads `collStats.getInteger("nindexes") > 0` unguarded and throws NPE.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: Unexpected exception type thrown ==> expected: <org.apache.kafka.connect.errors.DataException> but was: <java.lang.NullPointerException>
    at app//com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest.testSinkRegexTimeseriesMissingTimefield(MongoSinkTaskIntegrationTest.java:442)
    at com.mongodb.kafka.connect.util.TimeseriesValidation.shouldCreateCollection(TimeseriesValidation.java:249)
    at com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection(TimeseriesValidation.java:118)
    at com.mongodb.kafka.connect.sink.StartedMongoSinkTask.checkTimeseries(StartedMongoSinkTask.java:187)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. This is as much a connector robustness bug as a Firestore gap — `nindexes` is not guaranteed by the `collStats` contract, and a null-safe read would produce the intended validation error instead of an NPE. Time series collections are separately unsupported (`Unsupported fields in createCollection request: [timeseries]`), so this test could not pass even with `nindexes` present. Not fixed: `src/main/` is out of scope.

### Test: `MongoSinkTaskIntegrationTest.testSinkRegexTimeseriesWorks`

* **Display name:** Ensure sink regex timeseries works as expected
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** `collStats` omits `nindexes`. Firestore's reply is `{ok, ns, count, storageSize, inlineScanEligibleFields}`; `TimeseriesValidation.java:249` reads `collStats.getInteger("nindexes") > 0` unguarded and throws NPE.
  * **Raw Error / Assertion Mismatch:**

    ```
    java.lang.NullPointerException: Cannot invoke "java.lang.Integer.intValue()" because the return value of "org.bson.Document.getInteger(Object)" is null
    at com.mongodb.kafka.connect.util.TimeseriesValidation.shouldCreateCollection(TimeseriesValidation.java:249)
    at com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection(TimeseriesValidation.java:118)
    at com.mongodb.kafka.connect.sink.StartedMongoSinkTask.checkTimeseries(StartedMongoSinkTask.java:187)
    at com.mongodb.kafka.connect.sink.StartedMongoSinkTask.bulkWriteBatch(StartedMongoSinkTask.java:143)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. This is as much a connector robustness bug as a Firestore gap — `nindexes` is not guaranteed by the `collStats` contract, and a null-safe read would produce the intended validation error instead of an NPE. Time series collections are separately unsupported (`Unsupported fields in createCollection request: [timeseries]`), so this test could not pass even with `nindexes` present. Not fixed: `src/main/` is out of scope.

### Test: `MongoSinkTaskIntegrationTest.testBulkWriteOperationErrorWriteModelsIncludedInTheLog`

* **Display name:** Ensure bulk write operation error write models are included in the log
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (40.9s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSinkTaskIntegrationTest.testSinkCanHandleTombstoneNullEvents`

* **Display name:** Ensure sink can handle Tombstone null events
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (41.0s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `MongoSinkTaskIntegrationTest.testSinkProcessesSinkRecords`

* **Display name:** Ensure sink processes data from Kafka
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)
  * ad-hoc databases pinned to `org.mongodb.test.database`
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (41.5s). No behaviour difference observed.
* **Notes / Docs Reference:** —

## `com.mongodb.kafka.connect.ConnectorValidationIntegrationTest`

23 tests — 8 PASS, 3 FAIL, 12 BLOCKED. Results from `fs-results8/`.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationTimeseries`

* **Display name:** Ensure sink timeseries validation works as expected
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * **Failure Reason:** `collStats` omits `nindexes`. Firestore's reply is `{ok, ns, count, storageSize, inlineScanEligibleFields}`; `TimeseriesValidation.java:249` reads `collStats.getInteger("nindexes") > 0` unguarded and throws NPE.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: Sink had invalid configuration: ['timeseries.timefield': [Cannot invoke "java.lang.Integer.intValue()" because the return value of "org.bson.Document.getInteger(Object)" is null]] ==> expected: <true> but was: <false>
    at app//com.mongodb.kafka.connect.ConnectorValidationIntegrationTest.assertValidSink(ConnectorValidationIntegrationTest.java:549)
    at app//com.mongodb.kafka.connect.ConnectorValidationIntegrationTest.testSinkConfigValidationTimeseries(ConnectorValidationIntegrationTest.java:258)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. This is as much a connector robustness bug as a Firestore gap — `nindexes` is not guaranteed by the `collStats` contract, and a null-safe read would produce the intended validation error instead of an NPE. Time series collections are separately unsupported (`Unsupported fields in createCollection request: [timeseries]`), so this test could not pass even with `nindexes` present. Not fixed: `src/main/` is out of scope.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationTimeseriesRegex`

* **Display name:** Ensure sink timeseries validation works as expected when using regex config
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * **Failure Reason:** `collStats` omits `nindexes`. Firestore's reply is `{ok, ns, count, storageSize, inlineScanEligibleFields}`; `TimeseriesValidation.java:249` reads `collStats.getInteger("nindexes") > 0` unguarded and throws NPE.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: Sink had invalid configuration: ['timeseries.timefield': [Cannot invoke "java.lang.Integer.intValue()" because the return value of "org.bson.Document.getInteger(Object)" is null]] ==> expected: <true> but was: <false>
    at app//com.mongodb.kafka.connect.ConnectorValidationIntegrationTest.assertValidSink(ConnectorValidationIntegrationTest.java:549)
    at app//com.mongodb.kafka.connect.ConnectorValidationIntegrationTest.testSinkConfigValidationTimeseriesRegex(ConnectorValidationIntegrationTest.java:287)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. This is as much a connector robustness bug as a Firestore gap — `nindexes` is not guaranteed by the `collStats` contract, and a null-safe read would produce the intended validation error instead of an NPE. Time series collections are separately unsupported (`Unsupported fields in createCollection request: [timeseries]`), so this test could not pass even with `nindexes` present. Not fixed: `src/main/` is out of scope.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationTimeseriesRegexWithOverrides`

* **Display name:** Ensure sink timeseries validation works as expected when using regex config with overrides
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * **Failure Reason:** `collStats` omits `nindexes`. Firestore's reply is `{ok, ns, count, storageSize, inlineScanEligibleFields}`; `TimeseriesValidation.java:249` reads `collStats.getInteger("nindexes") > 0` unguarded and throws NPE.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: Sink had invalid configuration: ['timeseries.timefield': [Cannot invoke "java.lang.Integer.intValue()" because the return value of "org.bson.Document.getInteger(Object)" is null]] ==> expected: <true> but was: <false>
    at app//com.mongodb.kafka.connect.ConnectorValidationIntegrationTest.assertValidSink(ConnectorValidationIntegrationTest.java:549)
    at app//com.mongodb.kafka.connect.ConnectorValidationIntegrationTest.testSinkConfigValidationTimeseriesRegexWithOverrides(ConnectorValidationIntegrationTest.java:314)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. This is as much a connector robustness bug as a Firestore gap — `nindexes` is not guaranteed by the `collStats` contract, and a null-safe read would produce the intended validation error instead of an NPE. Time series collections are separately unsupported (`Unsupported fields in createCollection request: [timeseries]`), so this test could not pass even with `nindexes` present. Not fixed: `src/main/` is out of scope.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigAuthValidationTimeseries`

* **Display name:** Ensure sink validation timeseries auth permissions
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationCollectionBasedDifferentAuthPrivileges`

* **Display name:** Ensure sink validation passes with specific collection based privileges with a different auth db
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationCollectionBasedPrivileges`

* **Display name:** Ensure sink validation passes with specific collection based privileges
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationReadUser`

* **Display name:** Ensure sink validation fails with read user
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationReadWriteOnSpecificDatabase`

* **Display name:** Ensure sink validation passes with readWrite user on specific db
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationReadWriteUser`

* **Display name:** Ensure sink validation passes with readWrite user
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationTimeseriesNotSupported`

* **Display name:** Ensure sink validation when timeseries not supported
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** **Not Firestore-related.** An original connector version gate that does not apply here; the test is written for a server older than the one under test.
* **Notes / Docs Reference:** Pre-existing `assumeFalse(...)` in the unmodified test. Unrelated to compatibility.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationWithInvalidServerApi`

* **Display name:** Ensure sink configuration validation works with invalid serverApi
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** **Not Firestore-related.** An original connector version gate that does not apply here; the test is written for a server older than the one under test.
* **Notes / Docs Reference:** Pre-existing `assumeFalse(...)` in the unmodified test. Unrelated to compatibility.

### Test: `ConnectorValidationIntegrationTest.testSourceConfigValidationCollectionBasedPrivileges`

* **Display name:** Ensure source validation passes with specific collection based privileges
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSourceConfigValidationReadUser`

* **Display name:** Ensure source validation passes with read user
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSourceConfigValidationReadUserOnSpecificDatabase`

* **Display name:** Ensure source validation passes with read user on specific db
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** The test provisions a user or role to exercise the connector's permission validation. User and role management is administered through Google Cloud IAM, not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported commands. `runUserManagementCommand()` converts only that specific failure into a JUnit assumption, with the server's own error attached.
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Leaves the connector's auth/permission validation **untested** on Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through Google Cloud and supplying their credentials would recover this test.

### Test: `ConnectorValidationIntegrationTest.testSourceConfigValidationWithInvalidServerApi`

* **Display name:** Ensure source configuration validation works with invalid serverApi
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** **Not Firestore-related.** An original connector version gate that does not apply here; the test is written for a server older than the one under test.
* **Notes / Docs Reference:** Pre-existing `assumeFalse(...)` in the unmodified test. Unrelated to compatibility.

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidation`

* **Display name:** Ensure sink configuration validation works
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (0.5s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationInvalidConnection`

* **Display name:** Ensure sink configuration validation handles invalid connections
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (6.3s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationInvalidUser`

* **Display name:** Ensure sink configuration validation handles invalid user
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (42.4s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `ConnectorValidationIntegrationTest.testSinkConfigValidationWithServerApi`

* **Display name:** Ensure sink configuration validation works with serverApi
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (0.6s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `ConnectorValidationIntegrationTest.testSourceConfigValidation`

* **Display name:** Ensure source configuration validation works
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (0.8s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `ConnectorValidationIntegrationTest.testSourceConfigValidationInvalidConnection`

* **Display name:** Ensure source configuration validation handles invalid connections
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (1.7s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `ConnectorValidationIntegrationTest.testSourceConfigValidationInvalidUser`

* **Display name:** Ensure source configuration validation handles invalid user
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (11.0s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `ConnectorValidationIntegrationTest.testSourceConfigValidationWithValidServerApi`

* **Display name:** Ensure source configuration validation works with serverApi
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * `DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid `<db>#MongoKafkaTest` name that Firestore rejected outright)
  * `dropDatabases()` teardown drops collections instead of databases
  * `createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an unsupported-command failure into a skip
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (0.8s). No behaviour difference observed.
* **Notes / Docs Reference:** —

## `com.mongodb.kafka.connect.FullDocumentRoundTripIntegrationTest`

5 tests — 3 PASS, 2 FAIL, 0 BLOCKED. Results from `fs-results8/`.

### Test: `FullDocumentRoundTripIntegrationTest.testRoundTripInferSchemaValue`

* **Display name:** Ensure collection round trip inferring schema value
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * teardown drops collections instead of databases
  * source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only the `destination` collection is dropped on the target
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** Unsorted reads do not return documents in insertion order. Probe: inserting `_id` 1…20 in order and reading back gives `aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native MongoDB returns natural (insertion) order for a collection with no deletes; Firestore does not. The set of documents is correct — only the order differs.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable contents differ at index [5], expected: <{"_id": 6, "g": 3, "a": 2, "h": {"h1": 2, "h2": "2"}}> but was: <{"_id": 10, "h": ["1"]}>
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.lambda$assertCollection$9(MongoKafkaTestCase.java:449)
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.retry(MongoKafkaTestCase.java:461)
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.assertCollection(MongoKafkaTestCase.java:446)
    at app//com.mongodb.kafka.connect.FullDocumentRoundTripIntegrationTest.assertRoundTrip(FullDocumentRoundTripIntegrationTest.java:334)
    ```
* **Notes / Docs Reference:** https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences. Source side: `MongoCopyDataManager.copyDataFrom()` (`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its snapshot in arbitrary order. Sink side: the test reads the target collection back unsorted. Copy-existing ordering is not documented as guaranteed, so this is usable for consumers that do not depend on snapshot order.

### Test: `FullDocumentRoundTripIntegrationTest.testRoundTripSchema`

* **Display name:** Ensure collection round trip using Avro Schema
* **Status:** FAIL
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * teardown drops collections instead of databases
  * source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only the `destination` collection is dropped on the target
  * wire-version gate opened
* **Core Logic Result:**
  * **Failure Reason:** **Not a Firestore limitation — a harness artifact.** The sink writes nothing because the source task dies converting its first record: `Schema being registered is incompatible with an earlier schema for subject "copy.changestream.source-value", errorType: NAME_MISMATCH`. Upstream, each test in this class gets its own `getDatabaseWithPostfix()` database, so each gets its own topic and its own Schema Registry subject. Firestore cannot create databases on demand, so `org.mongodb.test.database` pins them all to one, the topic collapses to a single `copy.changestream.source`, and this test inherits the schema an earlier test in the class registered under that subject.
  * **Raw Error / Assertion Mismatch:**

    ```
    org.opentest4j.AssertionFailedError: iterable lengths differ, expected: <99> but was: <0>
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.lambda$assertCollection$9(MongoKafkaTestCase.java:449)
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.retry(MongoKafkaTestCase.java:465)
    at app//com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase.assertCollection(MongoKafkaTestCase.java:446)
    at app//com.mongodb.kafka.connect.FullDocumentRoundTripIntegrationTest.assertRoundTrip(FullDocumentRoundTripIntegrationTest.java:334)
    ```
* **Notes / Docs Reference:** The round trip itself is **untested** for the explicit-Avro-schema case. Recoverable by giving each test in the class a distinct collection name (the same trick `getAndCreateCollection()` already uses when the database is pinned), at the cost of provisioning a change stream per collection group. Not done here: it would turn a red test green, which is exactly the change this harness does not make without it being called out first.

### Test: `FullDocumentRoundTripIntegrationTest.testRoundTripBSON`

* **Display name:** Ensure collection round trip using BSON
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * teardown drops collections instead of databases
  * source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only the `destination` collection is dropped on the target
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (97.1s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `FullDocumentRoundTripIntegrationTest.testRoundTripDefault`

* **Display name:** Ensure collection round trip default settings
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * teardown drops collections instead of databases
  * source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only the `destination` collection is dropped on the target
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (103.3s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `FullDocumentRoundTripIntegrationTest.testRoundTripSimpleJsonFormat`

* **Display name:** Ensure collection round trip simple json format settings
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * teardown drops collections instead of databases
  * source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only the `destination` collection is dropped on the target
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (96.7s). No behaviour difference observed.
* **Notes / Docs Reference:** —

## `com.mongodb.kafka.connect.ChangeStreamRoundTripTest`

3 tests — 2 PASS, 0 FAIL, 1 BLOCKED. Results from `fs-results8/`.

### Test: `ChangeStreamRoundTripTest.testPipelineBasedUpdatesCanBeRoundTripped`

* **Display name:** Ensure collection CRUD operations are replicated
* **Status:** BLOCKED
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * teardown drops collections instead of databases
  * source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only `coll1` and `coll2` are dropped on the target
  * `assumeDistinctDatabases()` aborts if source and target resolve to the same database
  * wire-version gate opened
* **Core Logic Result:**
  * Not executed — aborted before the change stream logic could run.
  * **Blocking Reason:** **Not Firestore-related.** An original connector version gate that does not apply here; the test is written for a server older than the one under test.
* **Notes / Docs Reference:** Pre-existing `assumeFalse(...)` in the unmodified test. Unrelated to compatibility.

### Test: `ChangeStreamRoundTripTest.testRoundTripCollectionCrud`

* **Display name:** Ensure collection CRUD operations can be round tripped
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * teardown drops collections instead of databases
  * source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only `coll1` and `coll2` are dropped on the target
  * `assumeDistinctDatabases()` aborts if source and target resolve to the same database
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (100.7s). No behaviour difference observed.
* **Notes / Docs Reference:** —

### Test: `ChangeStreamRoundTripTest.testRoundTripDatabaseCrud`

* **Display name:** Ensure database CRUD operations can be round tripped
* **Status:** PASS
* **Setup/Teardown Modifications:**
  * collection-scope change streams provisioned ahead of the watch through the Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these tests run at all rather than skip
  * teardown drops collections instead of databases
  * source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only `coll1` and `coll2` are dropped on the target
  * `assumeDistinctDatabases()` aborts if source and target resolve to the same database
  * wire-version gate opened
* **Core Logic Result:**
  * Test completed successfully against Firestore Enterprise (187.7s). No behaviour difference observed.
* **Notes / Docs Reference:** —

<!-- END GENERATED PER-TEST DETAIL -->
