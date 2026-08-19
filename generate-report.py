#!/usr/bin/env python3
"""
Regenerates the per-test detail section of FIRESTORE_COMPATIBILITY_REPORT.md from the archived
JUnit XML, in the template the project instructions mandate ("Generate Detailed Test Reports
(MANDATORY)").

The narrative sections of the report are hand-written and are left alone. Only the block between
the BEGIN/END markers is replaced, so re-running this after a new suite run refreshes the per-test
detail without touching the analysis.

Usage:
    python3 generate-report.py                    # use the RUNS mapping below
    python3 generate-report.py fs-results4        # take every class from one run directory

Raw error text is redacted before it is written: endpoint hostnames and any credentialed
connection string are stripped, because this report is committed.
"""
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

REPORT = "FIRESTORE_COMPATIBILITY_REPORT.md"
BEGIN = "<!-- BEGIN GENERATED PER-TEST DETAIL -->"
END = "<!-- END GENERATED PER-TEST DETAIL -->"

SRC = "src/integrationTest/java/com/mongodb/kafka/connect"

# Class -> (result directory, source file). fs-results8 is the third sweep with collection scope
# change streams provisioned (FS_PROJECT set). Its MongoSourceTaskIntegrationTest was re-run after
# the provisioner was fixed to retry on an expired access token, so every test reaches the
# connector. Point at a different one with the command-line argument.
DEFAULT_RUN = "fs-results8"

RUNS = {
    "com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest": (
        DEFAULT_RUN, f"{SRC}/source/MongoSourceTaskIntegrationTest.java"),
    "com.mongodb.kafka.connect.MongoSourceConnectorIntegrationTest": (
        DEFAULT_RUN, f"{SRC}/MongoSourceConnectorIntegrationTest.java"),
    "com.mongodb.kafka.connect.MongoSinkConnectorIntegrationTest": (
        DEFAULT_RUN, f"{SRC}/MongoSinkConnectorIntegrationTest.java"),
    "com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest": (
        DEFAULT_RUN, f"{SRC}/sink/MongoSinkTaskIntegrationTest.java"),
    "com.mongodb.kafka.connect.ConnectorValidationIntegrationTest": (
        DEFAULT_RUN, f"{SRC}/ConnectorValidationIntegrationTest.java"),
    "com.mongodb.kafka.connect.FullDocumentRoundTripIntegrationTest": (
        DEFAULT_RUN, f"{SRC}/FullDocumentRoundTripIntegrationTest.java"),
    "com.mongodb.kafka.connect.ChangeStreamRoundTripTest": (
        DEFAULT_RUN, f"{SRC}/ChangeStreamRoundTripTest.java"),
}

ORDER = list(RUNS)

DOCS_CS = "https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/change-streams"
DOCS_DIFF = "https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/behavior-differences"

# Root causes. `blocked` marks a cause that stops the test before it can produce a compatibility
# verdict; those are reported BLOCKED rather than FAIL.
CAUSES = {
    "SHOW_EXPANDED_EVENTS": dict(
        blocked=False,
        reason="`showExpandedEvents` is not an accepted change stream option. The connector adds "
               "it to the `$changeStream` stage when `change.stream.show.expanded.events=true`, "
               "and the server rejects the aggregate with `invalid field(s) in change stream: "
               "[showExpandedEvents] 2`. The stream never opens, so the task polls zero records "
               "against an expected 3.",
        notes=f"{DOCS_CS}. The connector logs this as a resume failure "
              "(`StartedMongoSourceTask.java:471`) and keeps retrying rather than surfacing it, so "
              "the test sees an empty poll rather than an error. `disambiguatedPaths` itself is "
              "therefore **untested** — it is carried by the expanded event the option enables."),
    "TRUNCATED_ARRAYS": dict(
        blocked=False,
        reason="`updateDescription.truncatedArrays` is not populated. An update that shortens an "
               "array from 11 to 10 elements produces an update event whose `truncatedArrays` is "
               "`null` where MongoDB reports `[{field: items, newSize: 10}]`. The event itself "
               "arrives and `operationType` is correct — only this field of the payload differs.",
        notes=f"{DOCS_DIFF}. A payload-level difference, not a scope or provisioning gap: the "
              "change stream opened and delivered all three events. Consumers that read "
              "`truncatedArrays` to reconstruct array mutations will not get it from Firestore."),
    "RESUME_TOKEN_CODE": dict(
        blocked=False,
        reason="A resume token the server will not accept is reported as "
               "`Internal error encountered. 1`, not as MongoDB's `260` "
               "(`ChangeStreamFatalError` / invalidated resume token) or `286` "
               "(`ChangeStreamHistoryLost`). `StartedMongoSourceTask` keys its recovery on those "
               "codes (`INVALID_CHANGE_STREAM_ERRORS`, "
               "`StartedMongoSourceTask.java:104`), so with `errors.tolerance=all` it never "
               "recognises the token as invalid, retries with the same token three times, logs "
               "`Unable to recreate the cursor` and leaves the cursor null. Every subsequent "
               "`poll()` returns nothing, so the 50 inserts that follow are never emitted.",
        notes=f"{DOCS_DIFF}. The connector's documented recovery from a stale offset "
              "(`errors.tolerance=all`) does not work against Firestore. The workarounds the "
              "connector prints alongside the error — a new `offset.partition.name`, or removing "
              "the offset from its storage — still apply, because they avoid presenting the bad "
              "token in the first place."),
    "PIPELINE_STAGE": dict(
        blocked=False,
        reason="A disallowed aggregation stage in `pipeline` is rejected as "
               "`Stage GROUP is not allowed in change stream 2` (error code `2`, "
               "`InvalidArgument`), where MongoDB returns code `20` (`IllegalOperation`). The "
               "connector matches on code `20` (`ILLEGAL_OPERATION_ERROR`, "
               "`StartedMongoSourceTask.java:98`) to raise its `Illegal $changeStream operation` "
               "guidance, so that path never fires: the task treats the rejection as a resume "
               "failure and retries (140 times in this run) instead of failing fast with the "
               "friendly message the test asserts on.",
        notes=f"{DOCS_CS}. The stage really is refused — that part matches MongoDB. Only the "
              "error code differs, and the connector's diagnostics are keyed on the code. A "
              "misconfigured pipeline therefore presents as a silently stalled connector rather "
              "than a clear error."),
    "DOC_SIZE_LIMIT": dict(
        blocked=False,
        reason="The document the test writes to force an oversized change event is refused by the "
               "write itself: `WriteError{code=2, message='entity is too big'}`. Firestore's "
               "per-entity limit is well below MongoDB's 16 MB BSON limit, so the test cannot "
               "even construct the precondition it needs.",
        notes=f"{DOCS_DIFF}. What is left **untested** is the connector's `errors.tolerance=all` "
              "handling of a change event over 16 MB (`BSON_OBJECT_TOO_LARGE`, error `10334`); "
              "the size limit that would trigger it is unreachable here. The relevant finding is "
              "the write limit itself, which applies to the sink connector too."),
    # Retired: no longer attributed to any test. Kept because it documents why the declared
    # deviation exists, and the failure mode returns for any test that replays from Instant.EPOCH
    # against a pinned database whose change stream history outlives the run.
    "STALE_STREAM_HISTORY": dict(
        blocked=False,
        reason="**Not a Firestore limitation — a harness artifact.** The test configures "
               "`startAtOperationTime` as `Instant.EPOCH`, and Firestore clamps that to the oldest "
               "event it still retains for the collection group. Under one-database pinning the "
               "group `coll` is shared by six tests and its change stream history is retained for "
               "24h **across runs**, so the first event replayed was an `invalidate` with "
               "`clusterTime 2026-08-18T12:08:22.552Z` — 90 minutes before this run, left by the "
               "previous sweep's teardown drop of `coll`. An `invalidate` closes the cursor; the "
               "connector reinitialises at the same configured operation time "
               "(`StartedMongoSourceTask.java:426`), reads the same `invalidate`, and discards it "
               "because an `invalidate` carries no `ns` and so maps to no topic "
               "(`StartedMongoSourceTask.java:236`). Zero records against an expected 1, task dead "
               "after 355ms, the test's own insert never reached.",
        notes="Upstream this cannot happen: `getDatabaseWithPostfix()` hands out a fresh database "
              "per test, so the replay window holds only the test's own create and insert. "
              "**Fixed since this run** — the test now takes its start time from the clock after "
              "its own drop instead of from the epoch, and passes. That is the harness's one "
              "declared deviation (see the harness documentation, §\"Declared deviation\"), "
              "because it changes a "
              "value the connector consumes. What the fixed test then shows is a positive: "
              "Firestore honours `startAtOperationTime` and replays retained history."),
    # Currently unattributed, and enforced so by validate(): any test whose failure carries the 401
    # signature must be attributed here, which fails generation rather than letting a stale
    # Firestore cause be rendered for a test that never reached the connector. Kept because the
    # failure mode is environmental and can recur.
    "PROVISIONER_AUTH": dict(
        blocked=False,
        reason="**Not a Firestore limitation — a harness defect.** The test failed inside "
               "`FirestoreChangeStreamProvisioner`, before touching the connector: "
               "`GET .../changeStreams failed with HTTP 401: Request had invalid authentication "
               "credentials`, `reason: ACCESS_TOKEN_EXPIRED`. The provisioner cached the `gcloud "
               "auth print-access-token` result on a fixed TTL measured from its own fetch, but "
               "gcloud returns a token from *its* cache that may already be near expiry, so the "
               "TTL could not bound the token's real remaining life.",
        notes="No compatibility signal either way; a test that dies here is **untested**, not "
              "incompatible. Fixed since: the provisioner now discards the cached token and "
              "retries once on a 401 "
              "(`FirestoreChangeStreamProvisioner.java` §`request`), which does not depend on "
              "predicting the token's lifetime. Observed in fs-results8, where five tests in "
              "`MongoSourceTaskIntegrationTest` died this way; re-running the class after the fix "
              "cleared all five."),
    "SUBJECT_COLLISION": dict(
        blocked=False,
        reason="**Not a Firestore limitation — a harness artifact.** The sink writes nothing "
               "because the source task dies converting its first record: "
               "`Schema being registered is incompatible with an earlier schema for subject "
               "\"copy.changestream.source-value\", errorType: NAME_MISMATCH`. Upstream, each "
               "test in this class gets its own `getDatabaseWithPostfix()` database, so each gets "
               "its own topic and its own Schema Registry subject. Firestore cannot create "
               "databases on demand, so `org.mongodb.test.database` pins them all to one, the "
               "topic collapses to a single `copy.changestream.source`, and this test inherits "
               "the schema an earlier test in the class registered under that subject.",
        notes="The round trip itself is **untested** for the explicit-Avro-schema case. "
              "Recoverable by giving each test in the class a distinct collection name (the same "
              "trick `getAndCreateCollection()` already uses when the database is pinned), at the "
              "cost of provisioning a change stream per collection group. Not done here: it would "
              "turn a red test green, which is exactly the change this harness does not make "
              "without it being called out first."),
    "NINDEXES": dict(
        blocked=False,
        reason="`collStats` omits `nindexes`. Firestore's reply is "
               "`{ok, ns, count, storageSize, inlineScanEligibleFields}`; "
               "`TimeseriesValidation.java:249` reads `collStats.getInteger(\"nindexes\") > 0` "
               "unguarded and throws NPE.",
        notes=f"{DOCS_DIFF}. This is as much a connector robustness bug as a Firestore gap — "
              "`nindexes` is not guaranteed by the `collStats` contract, and a null-safe read "
              "would produce the intended validation error instead of an NPE. Time series "
              "collections are separately unsupported "
              "(`Unsupported fields in createCollection request: [timeseries]`), so this test "
              "could not pass even with `nindexes` present. Not fixed: `src/main/` is out of scope."),
    "PRE_POST_IMAGE": dict(
        blocked=False,
        reason="`changeStreamPreAndPostImages` is not an accepted `createCollection` option, so "
               "pre-/post-images cannot be enabled and `fullDocumentBeforeChange` is never "
               "populated.",
        notes=f"{DOCS_CS}. Affects `change.stream.full.document.before.change`."),
    "READ_ORDER": dict(
        blocked=False,
        reason="Unsorted reads do not return documents in insertion order. Probe: inserting `_id` "
               "1…20 in order and reading back gives "
               "`aggregate([]) -> 16,2,5,12,11,18,7,14,9,19,20,6,1,8,15,17,4,3,10,13`. Native "
               "MongoDB returns natural (insertion) order for a collection with no deletes; "
               "Firestore does not. The set of documents is correct — only the order differs.",
        notes=f"{DOCS_DIFF}. Source side: `MongoCopyDataManager.copyDataFrom()` "
              "(`src/main/java/com/mongodb/kafka/connect/source/MongoCopyDataManager.java:145`) "
              "runs `aggregate` with no `$sort`, so `startup.mode=copy_existing` emits its "
              "snapshot in arbitrary order. Sink side: the test reads the target collection back "
              "unsorted. Copy-existing ordering is not documented as guaranteed, so this is "
              "usable for consumers that do not depend on snapshot order."),
    # Currently unattributed: the target database was empty for the last run, so
    # testRoundTripDatabaseCrud passed. Kept because this depends on the state of the target
    # endpoint, not on the connector, and can recur on any run.
    "TARGET_NOT_EMPTY": dict(
        blocked=False,
        reason="**Not a Firestore limitation.** The test replicates a whole database and asserts "
               "the two match collection-for-collection, which requires an empty target. The "
               "target database used here holds 12 pre-existing collections, so the comparison "
               "trips on the first of them.",
        notes="Inherent to the test's design. A dedicated empty target database is needed for "
              "this test to produce a real verdict."),
    "COLL_SCOPE": dict(
        blocked=True,
        reason="Collection-scope change streams must be provisioned in advance. "
               "`db.getCollection(\"source\").watch()` returns `code 2 (InvalidArgument) :: "
               "Collection group scope change stream is not active for this collection group: "
               "'source'.` Skipped via "
               "`-Dorg.mongodb.test.skip.collection.change.streams=true`, which applies only when "
               "no project id is supplied to provision with.",
        notes=f"{DOCS_CS}. A **deployment gap, not a connector incompatibility**. The collection "
              "group does not need to exist when the stream is provisioned, so this is fully "
              "recoverable: re-run with `-Dorg.mongodb.test.firestore.project=<id>` (or "
              "`FS_PROJECT=<id>`) and the harness provisions each collection group through the "
              "Firestore Admin API and waits for it to activate, so the test runs instead of "
              "skipping."),
    "MULTI_DB": dict(
        blocked=True,
        reason="The test needs two or more *distinct* databases. Firestore cannot create a "
               "database on demand (`Invalid database name: changestream999`), so "
               "`org.mongodb.test.database` pins every ad-hoc database to the one that exists. "
               "The namespaces would collapse into one and the test would fail on "
               "`code 11000 Document already exists` — a harness artifact, not a Firestore "
               "signal — so `getDatabaseWithPostfix()` aborts on the second handout instead.",
        notes=f"{DOCS_DIFF}. These tests exist to prove that one connector watching the whole "
              "deployment fans in changes from several databases at once. A Firestore instance "
              "holds a single database, so that behaviour does not exist to be tested here — this "
              "is **out of scope rather than unsupported**, and no harness configuration recovers "
              "it."),
    "AUTH_USER": dict(
        blocked=True,
        reason="The test provisions a user or role to exercise the connector's permission "
               "validation. User and role management is administered through Google Cloud IAM, "
               "not the MongoDB wire protocol, so `createUser` / `createRole` are unsupported "
               "commands. `runUserManagementCommand()` converts only that specific failure into a "
               "JUnit assumption, with the server's own error attached.",
        notes=f"{DOCS_DIFF}. Leaves the connector's auth/permission validation **untested** on "
              "Firestore. Pre-provisioning `read`, `readWrite` and custom-role users through "
              "Google Cloud and supplying their credentials would recover this test."),
    "VERSION_GATE": dict(
        blocked=True,
        reason="**Not Firestore-related.** An original connector version gate that does not apply "
               "here; the test is written for a server older than the one under test.",
        notes="Pre-existing `assumeFalse(...)` in the unmodified test. Unrelated to compatibility."),
    "PASS": dict(blocked=False, reason=None, notes=None),
}

# method name -> cause key. Every test in the suite appears here; the generator fails loudly if a
# test in the XML is missing, so a new test cannot slip through unattributed.
ATTRIBUTION = {
    # --- MongoSourceTaskIntegrationTest ---
    "testSourceCanHandleNonExistentDatabaseAndSurviveDropping": "READ_ORDER",
    "testSourceCanHandleNonExistentDatabaseAndSurviveDroppingWithPipeline": "READ_ORDER",
    "testSourceTopicMapping": "READ_ORDER",
    # These four watched at deployment scope until org.mongodb.test.force.database.scope gave them
    # a source database. They now run, so they carry real verdicts rather than a scope failure.
    "testTruncatedArrays": "TRUNCATED_ARRAYS",
    "testDisambiguatedPathsExistWhenShowExpandedEventsIsTrue": "SHOW_EXPANDED_EVENTS",
    "testDisambiguatedPathsDontExistWhenShowExpandedEventsIsTrue": "PASS",
    "testDisambiguatedPathsDontExistByDefault": "PASS",
    "testFullDocumentBeforeChange": "PRE_POST_IMAGE",
    # Everything from here to the MULTI_DB pair watches at collection scope, so it was BLOCKED on
    # COLL_SCOPE until fs-results6 provisioned the change streams. These are the verdicts that run
    # produced.
    "testSourceLoadsDataFromCollectionDocumentOnly": "READ_ORDER",
    "testCopyingExistingWithARestartAfterFinishing": "READ_ORDER",
    "testCopyingExistingWithARestartMidwayThrough": "READ_ORDER",
    "testSourceCanHandleInvalidResumeTokenWhenErrorToleranceIsAll": "RESUME_TOKEN_CODE",
    "testErrorToleranceNoneSupport": "PASS",
    "testErrorToleranceAllSupport": "READ_ORDER",
    "testErrorToleranceAllSupport16MbError": "DOC_SIZE_LIMIT",
    # Was STALE_STREAM_HISTORY through fs-results7. The declared deviation moved the
    # configured start time off Instant.EPOCH, and the test now passes: Firestore honours
    # startAtOperationTime and replays retained history.
    "testStartAtOperationTime": "PASS",
    "testSourceCanHandleNonExistentCollectionAndSurviveDropping": "READ_ORDER",
    "testHonoursMaxBatchSize": "PASS",
    "testSourceEmitsNullValuesOnDelete": "READ_ORDER",
    "testSourceGeneratesHeartbeats": "PASS",
    "testDeadletterQueueHandling": "READ_ORDER",
    "testSourceCanUseCustomOffsetPartitionNames": "READ_ORDER",
    "testSourceLoadsDataFromMongoClient": "MULTI_DB",
    "testSourceLoadsDataFromMongoClientWithCopyExisting": "MULTI_DB",
    # --- MongoSourceConnectorIntegrationTest ---
    "testSchemaKeyAndValueOutput": "READ_ORDER",
    "testSourceUsesHeartbeatsForOffsets": "PASS",
    "testSourceHeartbeatsHaveValidSchema": "PASS",
    "testSourceLoadsDataFromCollectionCopyExistingBson": "READ_ORDER",
    "testSourceLoadsDataFromCollectionCopyExistingJson": "READ_ORDER",
    "testSourceHasFriendlyErrorMessagesForInvalidPipelines": "PIPELINE_STAGE",
    "testSourceLoadsDataFromCollectionCopyExistingByRegex": "MULTI_DB",
    # --- MongoSinkConnectorIntegrationTest (all pass) ---
    "testSinkSavesAvroDataToMongoDB": "PASS",
    "testSinkSavesAvroDataToMongoDBWhenUsingRegex": "PASS",
    "testSinkSurvivesARestart": "PASS",
    "testSinkSavesUsingMultipleTasksWithASinglePartition": "PASS",
    "testSinkSavesUsingASingleTasksWithMultiplePartitions": "PASS",
    "testSinkSavesUsingMultipleTasksWithMultiplePartitions": "PASS",
    "testSinkSavesToMultipleCollectionsUsingMultipleTasksWithMultiplePartitions": "PASS",
    # --- MongoSinkTaskIntegrationTest ---
    "testSinkProcessesSinkRecords": "PASS",
    "testSinkCanHandleTombstoneNullEvents": "PASS",
    "testBulkWriteOperationErrorWriteModelsIncludedInTheLog": "PASS",
    "testSinkCanHandleInvalidCDCWhenErrorToleranceIsAll": "READ_ORDER",
    "testSinkCanHandleInvalidKeyWhenErrorToleranceIsAll": "READ_ORDER",
    "testSinkCanHandleInvalidValueWhenErrorToleranceIsAll": "READ_ORDER",
    "testSinkCanHandleInvalidDocumentWhenErrorToleranceIsAll": "READ_ORDER",
    "testSinkProcessesTimeseriesData": "NINDEXES",
    "testSinkRegexTimeseriesWorks": "NINDEXES",
    "testSinkRegexTimeseriesMissingTimefield": "NINDEXES",
    "testSinkRegexTimeseriesCannotCreate": "NINDEXES",
    # --- ConnectorValidationIntegrationTest ---
    "testSourceConfigValidation": "PASS",
    "testSinkConfigValidation": "PASS",
    "testSourceConfigValidationInvalidUser": "PASS",
    "testSinkConfigValidationInvalidUser": "PASS",
    "testSourceConfigValidationInvalidConnection": "PASS",
    "testSinkConfigValidationInvalidConnection": "PASS",
    "testSourceConfigValidationWithValidServerApi": "PASS",
    "testSinkConfigValidationWithServerApi": "PASS",
    "testSinkConfigValidationTimeseries": "NINDEXES",
    "testSinkConfigValidationTimeseriesRegex": "NINDEXES",
    "testSinkConfigValidationTimeseriesRegexWithOverrides": "NINDEXES",
    "testSourceConfigValidationReadUser": "AUTH_USER",
    "testSourceConfigValidationReadUserOnSpecificDatabase": "AUTH_USER",
    "testSourceConfigValidationCollectionBasedPrivileges": "AUTH_USER",
    "testSinkConfigValidationReadUser": "AUTH_USER",
    "testSinkConfigValidationReadWriteUser": "AUTH_USER",
    "testSinkConfigValidationReadWriteOnSpecificDatabase": "AUTH_USER",
    "testSinkConfigValidationCollectionBasedPrivileges": "AUTH_USER",
    "testSinkConfigValidationCollectionBasedDifferentAuthPrivileges": "AUTH_USER",
    "testSinkConfigAuthValidationTimeseries": "AUTH_USER",
    "testSourceConfigValidationWithInvalidServerApi": "VERSION_GATE",
    "testSinkConfigValidationWithInvalidServerApi": "VERSION_GATE",
    "testSinkConfigValidationTimeseriesNotSupported": "VERSION_GATE",
    # --- FullDocumentRoundTripIntegrationTest ---
    "testRoundTripDefault": "PASS",
    "testRoundTripSimpleJsonFormat": "PASS",
    "testRoundTripInferSchemaValue": "READ_ORDER",
    "testRoundTripSchema": "SUBJECT_COLLISION",
    "testRoundTripBSON": "PASS",
    # --- ChangeStreamRoundTripTest ---
    # Passes whenever the target database is empty at the start of the run; re-attribute to
    # TARGET_NOT_EMPTY if a run finds pre-existing collections there.
    "testRoundTripDatabaseCrud": "PASS",
    "testRoundTripCollectionCrud": "PASS",
    "testPipelineBasedUpdatesCanBeRoundTripped": "VERSION_GATE",
}

# Harness modifications that apply to every test in a class.
CLASS_MODS = {
    "MongoSourceTaskIntegrationTest": [
        "collection-scope change streams provisioned ahead of the watch through the "
        "Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these "
        "tests run at all rather than skip",
        "`cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)",
        "ad-hoc databases pinned to `org.mongodb.test.database`",
        "wire-version gate opened (`getMaxWireVersion()` returns 7.0 unconditionally)",
        "`org.mongodb.test.force.database.scope` supplies a source database to a connector that "
        "left one unset, so it watches at database scope rather than the deployment scope "
        "Firestore does not have; a test that sets `database` itself is unaffected",
    ],
    "MongoSourceConnectorIntegrationTest": [
        "collection-scope change streams provisioned ahead of the watch through the "
        "Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these "
        "tests run at all rather than skip",
        "`cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)",
        "ad-hoc databases pinned to `org.mongodb.test.database`",
        "wire-version gate opened",
        "`org.mongodb.test.force.database.scope` supplies a source database to a connector that "
        "left one unset (see above)",
    ],
    "MongoSinkConnectorIntegrationTest": [
        "`cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)",
        "ad-hoc databases pinned to `org.mongodb.test.database`",
        "wire-version gate opened",
    ],
    "MongoSinkTaskIntegrationTest": [
        "`cleanUp()` drops each collection instead of calling `dropDatabase()` (unsupported)",
        "ad-hoc databases pinned to `org.mongodb.test.database`",
        "wire-version gate opened",
    ],
    "ConnectorValidationIntegrationTest": [
        "`DEFAULT_DATABASE_NAME` reads `org.mongodb.test.database` (the original built an invalid "
        "`<db>#MongoKafkaTest` name that Firestore rejected outright)",
        "`dropDatabases()` teardown drops collections instead of databases",
        "`createUser`/`createRole` routed through `runUserManagementCommand()`, which converts an "
        "unsupported-command failure into a skip",
    ],
    "FullDocumentRoundTripIntegrationTest": [
        "collection-scope change streams provisioned ahead of the watch through the "
        "Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these "
        "tests run at all rather than skip",
        "teardown drops collections instead of databases",
        "source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only the "
        "`destination` collection is dropped on the target",
        "wire-version gate opened",
    ],
    "ChangeStreamRoundTripTest": [
        "collection-scope change streams provisioned ahead of the watch through the "
        "Firestore Admin API (`org.mongodb.test.firestore.project`), which is what lets these "
        "tests run at all rather than skip",
        "teardown drops collections instead of databases",
        "source and target read from `org.mongodb.test.source.uri` / `…target.uri`; only `coll1` "
        "and `coll2` are dropped on the target",
        "`assumeDistinctDatabases()` aborts if source and target resolve to the same database",
        "wire-version gate opened",
    ],
}

# Extra per-test modifications beyond the class-wide ones.
TEST_MODS = {
    "testSourceCanUseCustomOffsetPartitionNames": [
        "up-front `skipIfCollectionScopeChangeStream(\"coll\")` — the shared check runs inside "
        "`assertDoesNotThrow(...)` here, which would turn the abort into a FAILED rather than a "
        "SKIPPED"],
    "testSourceCanHandleNonExistentDatabaseAndSurviveDropping": [
        "the in-test-body `db.drop()` is **deliberately left in place** — this test exercises "
        "change stream behaviour across a database drop, so the drop is test logic, not teardown"],
    "testSourceCanHandleNonExistentDatabaseAndSurviveDroppingWithPipeline": [
        "the in-test-body `db.drop()` is **deliberately left in place** — see above"],
    "testSourceCanHandleNonExistentCollectionAndSurviveDropping": [
        "the in-test-body `db.drop()` is **deliberately left in place** — see above"],
}

STATUS_LABEL = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "BLOCKED"}


def redact(text):
    """Strip anything that identifies the endpoint or carries a credential."""
    if not text:
        return text
    text = re.sub(r"mongodb(\+srv)?://[^\s'\"]+", "mongodb://<redacted>", text)
    text = re.sub(r"[0-9a-f]{8}-[0-9a-f-]+\.[a-z0-9-]+\.firestore\.goog(:\d+)?",
                  "<endpoint>", text)
    text = re.sub(r"[\w.-]*\.firestore\.goog(:\d+)?", "<endpoint>", text)
    # The Admin API URL carries the Google Cloud project id, which identifies the environment.
    text = re.sub(r"(firestore\.googleapis\.com/v\d+/projects/)[^/\s]+", r"\1<project>", text)
    return text


def display_to_method(path):
    """Map @DisplayName -> method name. Handles the name being on the following line."""
    text = open(path).read()
    lines = text.split("\n")
    out = {}
    for i, line in enumerate(lines):
        if "@DisplayName" not in line:
            continue
        window = "\n".join(lines[i:i + 3])
        name = re.search(r'@DisplayName\(\s*"((?:[^"\\]|\\.)*)"', window, re.S)
        if not name:
            continue
        for j in range(i + 1, min(i + 8, len(lines))):
            meth = re.match(r"\s*(?:public\s+)?void\s+(\w+)\(", lines[j])
            if meth:
                out[name.group(1)] = meth.group(1)
                break
    return out


def collect(runs):
    rows = []
    for fq in ORDER:
        run_dir, src_path = runs[fq]
        short = fq.rsplit(".", 1)[1]
        d2m = display_to_method(src_path)
        files = glob.glob(os.path.join(run_dir, "xml", short, "*.xml"))
        if not files:
            raise SystemExit(f"no XML for {short} under {run_dir}/xml/{short}")
        for path in files:
            root = ET.parse(path).getroot()
            for tc in root.iter("testcase"):
                display = tc.get("name")
                method = d2m.get(display, display.replace("()", ""))
                fails = list(tc.iter("failure")) + list(tc.iter("error"))
                skips = list(tc.iter("skipped"))
                status = "FAIL" if fails else ("SKIP" if skips else "PASS")
                node = fails[0] if fails else None
                rows.append(dict(
                    fq=fq, short=short, method=method, display=display, status=status,
                    time=float(tc.get("time", 0) or 0), run_dir=run_dir,
                    message=redact(node.get("message") or "") if node is not None else "",
                    trace=redact(node.text or "") if node is not None else "",
                ))
    return rows


def frames(trace):
    """
    The connector/test frames of a stacktrace. Prefer `com.mongodb.kafka` — that is the connector
    and the test itself; everything else is driver internals, JUnit and Gradle plumbing. Fall back
    to driver frames only when the connector does not appear.
    """
    stack = [ln.strip() for ln in trace.split("\n") if ln.strip().startswith("at ")]
    keep = [ln for ln in stack if "com.mongodb.kafka" in ln]
    if not keep:
        keep = [ln for ln in stack if "com.mongodb." in ln]
    return keep[:4]


# A test that dies in the provisioner never reaches the connector, so whatever cause it is
# attributed to did not actually run. Polarity alone does not catch this: the test still failed, so
# a stale Firestore attribution passes the PASS/FAIL checks and is rendered as a real finding.
HARNESS_401 = re.compile(r"ACCESS_TOKEN_EXPIRED|changeStreams failed with HTTP 401")


def validate(rows):
    """
    Check every attribution against the run before rendering anything, and report all the problems
    at once. Failing on the first one turns a stale ATTRIBUTION table into a fix-one-rerun loop,
    which is slow when a harness change flips several tests at the same time.
    """
    problems = []
    for r in rows:
        key = ATTRIBUTION.get(r["method"])
        label = f"{r['short']}.{r['method']}"
        if key is None:
            problems.append(f"{label} ({r['status']}) is unattributed — add it to ATTRIBUTION")
        elif key not in CAUSES:
            problems.append(f"{label} is attributed to {key}, which is not in CAUSES")
        elif r["status"] == "PASS" and key != "PASS":
            problems.append(f"{label} passed but is attributed to {key} — retire the attribution")
        elif r["status"] != "PASS" and key == "PASS":
            problems.append(f"{label} is {r['status']} but is attributed to PASS — give it a cause")
        elif HARNESS_401.search(r["message"] + r["trace"]) and key != "PROVISIONER_AUTH":
            problems.append(f"{label} died on the provisioner's expired access token but is "
                            f"attributed to {key} — that cause did not run. Re-mint the token and "
                            f"re-run the class; do not re-attribute to PROVISIONER_AUTH to get "
                            f"past this, as it records a Firestore finding as untested.")
    if problems:
        raise SystemExit("ATTRIBUTION does not match the run:\n  "
                         + "\n  ".join(problems)
                         + f"\n\n{len(problems)} problem(s). The report was not written.")

    unused = sorted(set(CAUSES) - {ATTRIBUTION[r["method"]] for r in rows})
    if unused:
        print(f"note: causes not used by this run: {', '.join(unused)}")


def render(rows):
    out = [BEGIN, "",
           "> Generated by `generate-report.py` from the archived JUnit XML. Edits here are",
           "> overwritten — change the script or the narrative sections above instead.", ""]
    for fq in ORDER:
        group = [r for r in rows if r["fq"] == fq]
        short = fq.rsplit(".", 1)[1]
        p = sum(1 for r in group if r["status"] == "PASS")
        f = sum(1 for r in group if r["status"] == "FAIL")
        s = sum(1 for r in group if r["status"] == "SKIP")
        out.append(f"## `{fq}`")
        out.append("")
        out.append(f"{len(group)} tests — {p} PASS, {f} FAIL, {s} BLOCKED. "
                   f"Results from `{group[0]['run_dir']}/`.")
        out.append("")
        order = {"FAIL": 0, "SKIP": 1, "PASS": 2}
        for r in sorted(group, key=lambda r: (order[r["status"]], r["method"])):
            cause = CAUSES[ATTRIBUTION[r["method"]]]

            out.append(f"### Test: `{short}.{r['method']}`")
            out.append("")
            out.append(f"* **Display name:** {r['display']}")
            out.append(f"* **Status:** {STATUS_LABEL[r['status']]}")
            mods = CLASS_MODS[short] + TEST_MODS.get(r["method"], [])
            out.append("* **Setup/Teardown Modifications:**")
            for m in mods:
                out.append(f"  * {m}")
            out.append("* **Core Logic Result:**")
            if r["status"] == "PASS":
                out.append(f"  * Test completed successfully against Firestore Enterprise "
                           f"({r['time']:.1f}s). No behaviour difference observed.")
            elif r["status"] == "SKIP":
                out.append("  * Not executed — aborted before the change stream logic could run.")
                out.append(f"  * **Blocking Reason:** {cause['reason']}")
            else:
                out.append(f"  * **Failure Reason:** {cause['reason']}")
                out.append("  * **Raw Error / Assertion Mismatch:**")
                out.append("")
                out.append("    ```")
                for ln in r["message"].split("\n")[:8]:
                    out.append(f"    {ln.strip()}")
                for ln in frames(r["trace"]):
                    out.append(f"    at {ln}" if not ln.startswith("at ") else f"    {ln}")
                out.append("    ```")
            if cause["notes"]:
                out.append(f"* **Notes / Docs Reference:** {cause['notes']}")
            else:
                out.append("* **Notes / Docs Reference:** —")
            out.append("")
    out.append(END)
    return "\n".join(out)


def scaffold():
    """
    A report with the markers but no analysis, written when the report file is absent.

    Part A is hand-written and cannot be regenerated from the XML, so the only thing this can
    honestly produce is an empty frame plus a warning. That still beats a traceback: the per-test
    detail the run just produced is worth keeping, and losing it because the narrative half is
    missing helps nobody.
    """
    return "\n".join([
        "# MongoDB Kafka Connector vs Firestore Enterprise Change Streams",
        "",
        "> **Part A is missing.** This file was regenerated from the JUnit XML after the report",
        "> went absent, so only the generated per-test detail below is present. The analysis —",
        "> headline table, what works, root causes, what the skips leave untested — is",
        "> hand-written and has to be restored by hand. See the report template in the project",
        "> instructions.",
        "",
        "---",
        "",
        BEGIN,
        END,
        "",
    ])


def main():
    runs = RUNS
    if len(sys.argv) > 1:
        one = sys.argv[1].rstrip("/")
        runs = {fq: (one, src) for fq, (_, src) in RUNS.items()}
    rows = collect(runs)
    validate(rows)

    body = render(rows)
    if os.path.exists(REPORT):
        text = open(REPORT).read()
    else:
        text = scaffold()
        print(f"warning: {REPORT} did not exist — created it with the generated detail only. "
              f"Part A (the hand-written analysis) is NOT recoverable from the XML and must be "
              f"rewritten.")
    if BEGIN not in text or END not in text:
        raise SystemExit(f"{REPORT} is missing the {BEGIN} / {END} markers")
    head, rest = text.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    open(REPORT, "w").write(head + body + tail)

    p = sum(1 for r in rows if r["status"] == "PASS")
    f = sum(1 for r in rows if r["status"] == "FAIL")
    s = sum(1 for r in rows if r["status"] == "SKIP")
    print(f"{REPORT}: {len(rows)} tests — {p} PASS, {f} FAIL, {s} BLOCKED")


if __name__ == "__main__":
    main()
