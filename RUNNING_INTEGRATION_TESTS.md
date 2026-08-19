# Running the integration tests

How to execute the MongoDB Kafka Connector integration suite — against Firestore Enterprise (this
project's purpose) or against a native MongoDB replica set.

For *why* the harness is shaped the way it is, and what each modification does, see the harness
documentation. For the results of the last run, see `FIRESTORE_COMPATIBILITY_REPORT.md`.

Paths, database ids, project ids and output directories below are placeholders in angle brackets —
substitute your own.

---

## 1. The one thing to get right first

Integration tests live in their **own source set and Gradle task**.

| Task | What it runs | Needs a server |
|---|---|---|
| `./gradlew test` | unit tests only | no |
| `./gradlew integrationTest` | the integration suite | **yes** |
| `./gradlew check` | compile + spotless + checkstyle + spotbugs + both test tasks | yes |

`./gradlew test` will never exercise Firestore, no matter which properties you pass it.

## 2. Prerequisites

**JDK 17.** The build compiles Java 8 bytecode with a JDK 17 toolchain, and the Gradle daemon
itself must run on 17:

```bash
export JAVA_HOME=<jdk-17-home>
```

If the ambient `JAVA_HOME` is an older JDK, this is not optional. See §7 for why it matters more
than it looks.

**No Docker.** Kafka Connect runs in-process; there is nothing to start.

**An authenticated `gcloud`** — only if you want the harness to provision collection-scope change
streams (§9). Without it, set `FS_SKIP_COLLECTION_SCOPE=true` and those 25 tests report SKIPPED.

**Credentials** (Firestore runs only). Keep the two connection strings in a shell file **outside the
repo** — referred to below as `<env-file>` — and source it before a run. The URIs carry live
passwords, so never commit it:

```bash
cat > <env-file> <<'EOF'
export FS_SOURCE_URI="mongodb://<user>:<pass>@<source-host>/<db>?..."
export FS_TARGET_URI="mongodb://<user>:<pass>@<target-host>/<db>?..."
EOF
chmod 600 <env-file>
```

**An empty target database**, for `ChangeStreamRoundTripTest` only. It replicates a whole database
and asserts the two match collection for collection, so any pre-existing collection on the target
fails the comparison. It must also be a *different* database from the source, or the test aborts
rather than report a vacuous pass. Both are inherent to the test, not Firestore limitations.

---

## 3. Full suite against Firestore

From the repository root:

```bash
. <env-file>
FIRESTORE_URI="$FS_SOURCE_URI" FS_PROJECT=<project-id> ./run-firestore-tests.sh <run-dir>
```

`run-firestore-tests.sh` runs the seven in-scope classes **one at a time**, archives the JUnit XML
and Gradle log per class, and prints a summary table at the end.

Drop `FS_PROJECT` to skip the collection-scope tests instead of provisioning for them — §9 has the
tradeoff.

It takes well over an hour — seven separate Gradle invocations, much of it spent waiting out
connector poll timeouts on the failing tests, plus the activation wait for any change stream the
harness has to create. Run it detached:

```bash
. <env-file>
FIRESTORE_URI="$FS_SOURCE_URI" FS_PROJECT=<project-id> \
  nohup ./run-firestore-tests.sh <run-dir> > <run-log> 2>&1 &
tail -f <run-log>
```

**Use a fresh output directory for each sweep.** The script appends into whatever directory you
name, so reusing one silently mixes runs. Anything matching `fs-results*/` is gitignored, so a
numbered series under that prefix is the convention here.

### Quick example

End to end, with two Firestore endpoints and provisioning enabled:

```bash
# 1. Credentials, kept outside the repo
cat > ~/.config/fs-uris.sh <<'EOF'
export FS_SOURCE_URI="mongodb://kafka-it:s3cr3t@abc123.us-central1.firestore.goog:443/source-db?tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false"
export FS_TARGET_URI="mongodb://kafka-it:s3cr3t@def456.us-central1.firestore.goog:443/target-db?tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false"
EOF
chmod 600 ~/.config/fs-uris.sh

# 2. Authenticate gcloud, so the harness can provision collection-scope streams
gcloud auth login

# 3. Run the sweep, detached
. ~/.config/fs-uris.sh
FIRESTORE_URI="$FS_SOURCE_URI" FS_PROJECT=my-gcp-project \
  setsid nohup ./run-firestore-tests.sh ./fs-results1 > /tmp/fs-run1.log 2>&1 < /dev/null &
disown

# 4. Watch it
tail -f /tmp/fs-run1.log
```

`FS_SOURCE_URI` and `FS_TARGET_URI` are picked up from the environment by the script; only
`FIRESTORE_URI` has to be set explicitly. `FIRESTORE_DB` defaults to the database in its path
(`source-db` here), so it does not need setting either.

The last thing it prints is the per-class summary:

```
=== Summary ===
ChangeStreamRoundTripTest                     tests=   3 passed=   2 failed=  0 errors=  0 skipped=   1
ConnectorValidationIntegrationTest            tests=  23 passed=   8 failed=  3 errors=  0 skipped=  12
FullDocumentRoundTripIntegrationTest          tests=   5 passed=   3 failed=  2 errors=  0 skipped=   0
MongoSinkConnectorIntegrationTest             tests=   7 passed=   7 failed=  0 errors=  0 skipped=   0
MongoSinkTaskIntegrationTest                  tests=  11 passed=   3 failed=  8 errors=  0 skipped=   0
MongoSourceConnectorIntegrationTest           tests=   9 passed=   2 failed=  4 errors=  0 skipped=   3
MongoSourceTaskIntegrationTest                tests=  24 passed=   6 failed= 16 errors=  0 skipped=   2
```

Sanity-check it before believing any of it: **seven rows, no `NO RESULTS`, and a non-zero
`tests=`**. A row reading `NO RESULTS` means that class never ran — read its log in the run
directory. All seven reading `NO RESULTS` almost always means `JAVA_HOME` (§7).

### Script environment variables

| Variable | Default | Maps to |
|---|---|---|
| `FIRESTORE_URI` | *required* | `-Dorg.mongodb.test.uri` |
| `FIRESTORE_DB` | the database in `FIRESTORE_URI`'s path | `-Dorg.mongodb.test.database` |
| `FS_SOURCE_URI` | `FIRESTORE_URI` | `-Dorg.mongodb.test.source.uri` |
| `FS_TARGET_URI` | `FIRESTORE_URI` | `-Dorg.mongodb.test.target.uri` |
| `FS_SKIP_COLLECTION_SCOPE` | `true` | `-Dorg.mongodb.test.skip.collection.change.streams` |
| `FS_PROJECT` | unset | `-Dorg.mongodb.test.firestore.project` — set it to provision collection-scope streams instead of skipping those tests |
| `FS_CHANGE_STREAM_RETENTION` | unset (`86400s`) | `-Dorg.mongodb.test.firestore.retention` |
| `FS_FORCE_DATABASE_SCOPE` | `true` | `-Dorg.mongodb.test.force.database.scope` — keep it on for Firestore, which has no deployment-scope change stream |
| `FS_JAVA_HOME` | a JDK 17 path baked into the script | `JAVA_HOME` for the Gradle daemon — override it if your JDK 17 lives elsewhere |

### Classes it runs

```
source.MongoSourceTaskIntegrationTest
MongoSourceConnectorIntegrationTest
MongoSinkConnectorIntegrationTest
sink.MongoSinkTaskIntegrationTest
ConnectorValidationIntegrationTest
FullDocumentRoundTripIntegrationTest
ChangeStreamRoundTripTest
```

Two classes are deliberately excluded:

* `MongoSourceTaskIntegrationTest2` — a Mockito unit test stranded in the `integrationTest` source
  set. Never reaches a server, so it carries no compatibility signal.
* `sink.CsfleIntegrationTest` — needs `crypt_shared` or `mongocryptd`. Where neither is installed
  all four tests self-skip, leaving CSFLE untested rather than shown to be unsupported.

---

## 4. A single class or a single test

Single class, the four single-endpoint classes:

```bash
. <env-file>
export JAVA_HOME=<jdk-17-home>
./gradlew integrationTest \
  -Dorg.mongodb.test.uri="$FS_SOURCE_URI" \
  -Dorg.mongodb.test.database=<database> \
  -Dorg.mongodb.test.firestore.project=<project-id> \
  -Dorg.mongodb.test.force.database.scope=true \
  --tests "com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest"
```

`<database>` is the existing Firestore database to pin to — normally the one in the URI's path.

`run-firestore-tests.sh` defaults `-Dorg.mongodb.test.force.database.scope` to `true`; by hand you
have to pass it. Without it the five deployment-scope tests in this class fail on
``unsupported database `admin` `` rather than returning a verdict — see §6.

The two round-trip classes need both endpoints:

```bash
./gradlew integrationTest \
  -Dorg.mongodb.test.uri="$FS_SOURCE_URI" \
  -Dorg.mongodb.test.source.uri="$FS_SOURCE_URI" \
  -Dorg.mongodb.test.target.uri="$FS_TARGET_URI" \
  -Dorg.mongodb.test.firestore.project=<project-id> \
  --tests "com.mongodb.kafka.connect.FullDocumentRoundTripIntegrationTest"
```

Swap `-Dorg.mongodb.test.firestore.project` for
`-Dorg.mongodb.test.skip.collection.change.streams=true` on an endpoint you cannot provision for.

Single test method:

```bash
--tests "com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest.testSinkProcessesTimeseriesData"
```

`--tests` matches the **method name**, not the `@DisplayName`. The JUnit XML and the console output
show display names, so map back through the source file (or through Part B of the compatibility
report, whose entries carry both).

---

## 5. Against native MongoDB

Only `org.mongodb.test.uri` is needed, and it must point at a **replica set or sharded cluster** —
the source connector needs change streams:

```bash
export JAVA_HOME=<jdk-17-home>
./gradlew integrationTest -Dorg.mongodb.test.uri="mongodb://localhost:27017/?replicaSet=rs0"
```

The other seven properties default to empty and fall back to the original upstream behaviour, so
none of the Firestore harness changes affect this run — nothing needs provisioning, nothing is
skipped, and the deployment-scope tests still watch the whole deployment.

---

## 6. Configuration properties

All eight are forwarded to the test JVM by `build.gradle.kts`. All are unset by default.

| Property | Purpose |
|---|---|
| `org.mongodb.test.uri` | The endpoint. Pre-existing upstream property. |
| `org.mongodb.test.database` | Pins the ad-hoc `<name><postfix>` databases to one existing database. Needed because Firestore cannot create a database on demand. |
| `org.mongodb.test.source.uri` | Source endpoint for the round-trip classes. A complete URI, not a database name. |
| `org.mongodb.test.target.uri` | Target endpoint for the round-trip classes. |
| `org.mongodb.test.skip.collection.change.streams` | `true` skips every test whose source connector watches at collection scope. |
| `org.mongodb.test.firestore.project` | Google Cloud project id. Setting it provisions the collection-group change streams those tests need, so they run instead of skipping. Overrides the property above. See §9. |
| `org.mongodb.test.firestore.retention` | Retention for streams the harness provisions. Defaults to `86400s`. |
| `org.mongodb.test.force.database.scope` | `true` gives a source database to any connector that left one unset, so it watches at database scope. Firestore rejects the deployment scope with ``unsupported database `admin` ``. A test that sets its own database is unaffected. |

Setting a property on the Gradle command line is not enough on its own — it must be in the
forwarding list in the `integrationTest` task in `build.gradle.kts` or it stays on the Gradle JVM
and never reaches the tests.

---

## 7. Five things that bite

**`JAVA_HOME` must be JDK 17 — verify it, don't assume.** On an older JDK the Gradle daemon refuses
to start (`Unrecognized option: --add-exports`). **No tests run at all**, and because the summary
still prints, a whole-suite run can look like it executed when it did not. `run-firestore-tests.sh` exports it for you; a manual `./gradlew` invocation does not.
Check with `java -version` after exporting, and confirm the test count is non-zero.

**Gradle leaves stale XML behind.** A class that dies before producing results silently inherits
the previous class's numbers from `build/test-results/integrationTest/`. The script clears them
between classes and prints `NO RESULTS` rather than a row of zeroes. If you run several classes by
hand, `rm -f build/test-results/integrationTest/*.xml` between them.

**A killed run leaves a JVM holding the schema registry port.** The next run then dies in
`initializationError` with `java.io.IOException: Failed to bind to /0.0.0.0:8081` for every class,
which looks nothing like a port problem in the summary. Kill the orphan and wait for the port
before restarting:

```bash
pkill -f GradleWorkerMain
until ! ss -ltn | grep -q ':8081 '; do sleep 2; done
```

Detaching properly avoids most of this — `setsid nohup ./run-firestore-tests.sh … < /dev/null &
disown`, not a plain background job, which dies with the shell part-way through a class.

**No `clean` needed.** The task sets `outputs.upToDateWhen { false }`, so it always re-executes.

**`spotlessApply` runs before `compileJava`.** Formatting drift is fixed silently on build — expect
it when reviewing diffs after a run.

---

## 8. Where results land

| Path | Contents |
|---|---|
| `build/test-results/integrationTest/*.xml` | JUnit XML — the machine-readable source of truth |
| `build/reports/tests/integrationTest/index.html` | browsable HTML report |
| `fs-results*/xml/<Class>/` | the script's per-class archive of the XML |
| `fs-results*/<Class>.log` | full Gradle output for that class |

`fs-results*/` is gitignored. The logs and XML echo the connection strings, which carry live
passwords — do not force-add them.

## 9. Provisioning collection-scope change streams

Firestore will not open a collection-scope change stream unless one has been provisioned for that
**collection group** out of band. Without it, `db.getCollection("coll1").watch()` fails with:

```
code 2 (InvalidArgument) :: Collection group scope change stream is not active for this
collection group: 'coll1'. Newly created change streams may take a few minutes to become active.
```

The collection group does **not** need to exist yet — provisioning `coll1` on an empty database
works, and the test can create the collection later. So this is a setup step, not a barrier.

### Let the harness do it

Point the suite at your project and it provisions each collection group on demand, waits for the
stream to go active, and runs the test:

```bash
export FS_PROJECT=<project-id>          # or -Dorg.mongodb.test.firestore.project=<project-id>
gcloud auth login                       # the token comes from `gcloud auth print-access-token`
```

`FirestoreChangeStreamProvisioner` handles it. It is idempotent and caches per JVM, so an
already-provisioned group costs one API call and the few-minute activation wait is paid once per
collection group, not once per test. `FS_PROJECT` overrides `FS_SKIP_COLLECTION_SCOPE`, so a run
that can provision never skips.

The first run against a fresh database is slow — every new collection group blocks for its
activation window. Pre-provisioning by hand (below) beforehand avoids that; the harness then finds
the streams already there.

### Or by hand

The public docs describe only the Cloud Console path. There is also an Admin API, which is what the
harness and the commands below use:

```bash
P=<project-id>
DB=<firestore-database-id>          # e.g. the database in FS_SOURCE_URI's path
TOKEN=$(gcloud auth print-access-token)

# Provision one collection group
create_cs() {
  curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    "https://firestore.googleapis.com/v1/projects/$P/databases/$DB/changeStreams?changeStreamId=kafka-it-$1" \
    -d "{\"collectionGroupScope\":{\"collectionGroupId\":\"$1\"},\"retentionPeriod\":\"86400s\"}"
}

# What the suite watches: fixed names, plus coll<N> from MongoKafkaTestCase's POSTFIX counter
for CG in source coll1 coll2 customCollection; do create_cs "$CG"; done
for N in $(seq 1 100); do create_cs "coll$N"; done
```

List and delete:

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://firestore.googleapis.com/v1/projects/$P/databases/$DB/changeStreams"

curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://firestore.googleapis.com/v1/projects/$P/databases/$DB/changeStreams/kafka-it-coll1"
```

**Wait for activation.** The create response carries a `startTime` roughly five minutes ahead of
`createTime`; the stream is not usable until then. Verify before running:

```bash
mongosh "$FS_SOURCE_URI" --quiet --eval \
  'try { db.getCollection("coll1").watch().close(); print("ACTIVE") } catch(e) { print(e.message) }'
```

Then run with the skip **off**, so those tests actually execute:

```
-Dorg.mongodb.test.skip.collection.change.streams=false
```

Leave it `true` only for an endpoint where neither path is available — it converts those 25 tests
into SKIPPED, which is untested, not shown-unsupported.

## 10. Refreshing the compatibility report

After a clean sweep, regenerate Part B of `FIRESTORE_COMPATIBILITY_REPORT.md` from that one run
directory:

```bash
python3 generate-report.py <run-dir>
```

With no argument it uses the run directory named in the script's `DEFAULT_RUN`.

The generator fails loudly if a test in the XML is missing from its `ATTRIBUTION` table, or if an
attribution disagrees with the observed status. Part A's numbers and narrative are hand-maintained.
