#!/usr/bin/env bash
#
# Runs the in-scope MongoDB Kafka Connector integration tests against a Firestore Enterprise
# endpoint, one class at a time, archiving the JUnit XML per class.
#
# The URIs carry live SCRAM passwords. Keep them out of the repo, out of committed logs, and out
# of the compatibility report.
#
# Usage:
#   . /tmp/fs-uris.sh
#   FIRESTORE_URI="$FS_SOURCE_URI" ./run-firestore-tests.sh ./fs-results
#
set -uo pipefail

RESULTS_DIR="${1:-./fs-results}"

if [ -z "${FIRESTORE_URI:-}" ]; then
  echo "FIRESTORE_URI is required" >&2
  exit 1
fi

# Default the pinned database to the one in FIRESTORE_URI's path.
if [ -z "${FIRESTORE_DB:-}" ]; then
  FIRESTORE_DB=$(printf '%s' "$FIRESTORE_URI" | sed -E 's#^[^/]*//[^/]*/##; s#\?.*$##')
fi
FS_SOURCE_URI="${FS_SOURCE_URI:-$FIRESTORE_URI}"
FS_TARGET_URI="${FS_TARGET_URI:-$FIRESTORE_URI}"
FS_SKIP_COLLECTION_SCOPE="${FS_SKIP_COLLECTION_SCOPE:-true}"
# Set FS_PROJECT to the Google Cloud project id to have the harness provision the collection group
# change streams the collection scope tests need, instead of skipping those tests. Requires gcloud
# to be authenticated. FS_SKIP_COLLECTION_SCOPE is then ignored.
FS_PROJECT="${FS_PROJECT:-}"
FS_CHANGE_STREAM_RETENTION="${FS_CHANGE_STREAM_RETENTION:-}"
# Firestore has no deployment scope change stream (MongoClient.watch() is an aggregate against
# `admin`, which Firestore does not have). Supply the source database when a test leaves it unset
# so the connector watches at database scope instead.
FS_FORCE_DATABASE_SCOPE="${FS_FORCE_DATABASE_SCOPE:-true}"

# The build targets JDK 17 and the Gradle daemon fails to start on the JDK 8 that is JAVA_HOME by
# default in this environment ("Unrecognized option: --add-exports"), so set it rather than
# defaulting it. Override with FS_JAVA_HOME.
export JAVA_HOME="${FS_JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"

# MongoSourceTaskIntegrationTest2 is a Mockito unit test stranded in the integrationTest source
# set, and sink.CsfleIntegrationTest needs crypt_shared/mongocryptd. Neither carries a Firestore
# compatibility signal, so both are excluded.
CLASSES=(
  "com.mongodb.kafka.connect.source.MongoSourceTaskIntegrationTest"
  "com.mongodb.kafka.connect.MongoSourceConnectorIntegrationTest"
  "com.mongodb.kafka.connect.MongoSinkConnectorIntegrationTest"
  "com.mongodb.kafka.connect.sink.MongoSinkTaskIntegrationTest"
  "com.mongodb.kafka.connect.ConnectorValidationIntegrationTest"
  "com.mongodb.kafka.connect.FullDocumentRoundTripIntegrationTest"
  "com.mongodb.kafka.connect.ChangeStreamRoundTripTest"
)

mkdir -p "$RESULTS_DIR"

for class in "${CLASSES[@]}"; do
  short="${class##*.}"
  echo "=== $short ==="
  # Clear previous results, otherwise a class that fails before producing XML silently inherits
  # the previous class's numbers.
  rm -f build/test-results/integrationTest/*.xml
  ./gradlew integrationTest \
    -Dorg.mongodb.test.uri="$FIRESTORE_URI" \
    -Dorg.mongodb.test.database="$FIRESTORE_DB" \
    -Dorg.mongodb.test.source.uri="$FS_SOURCE_URI" \
    -Dorg.mongodb.test.target.uri="$FS_TARGET_URI" \
    -Dorg.mongodb.test.skip.collection.change.streams="$FS_SKIP_COLLECTION_SCOPE" \
    -Dorg.mongodb.test.firestore.project="$FS_PROJECT" \
    -Dorg.mongodb.test.firestore.retention="$FS_CHANGE_STREAM_RETENTION" \
    -Dorg.mongodb.test.force.database.scope="$FS_FORCE_DATABASE_SCOPE" \
    --tests "$class" \
    > "$RESULTS_DIR/$short.log" 2>&1
  echo "  exit=$? (log: $RESULTS_DIR/$short.log)"

  if [ -d build/test-results/integrationTest ]; then
    mkdir -p "$RESULTS_DIR/xml/$short"
    cp build/test-results/integrationTest/*.xml "$RESULTS_DIR/xml/$short/" 2>/dev/null
  fi
done

echo
echo "=== Summary ==="
python3 - "$RESULTS_DIR/xml" <<'EOF'
import glob, os, sys
import xml.etree.ElementTree as ET

root = sys.argv[1]
for d in sorted(glob.glob(os.path.join(root, "*"))):
    t = f = s = e = 0
    for x in glob.glob(os.path.join(d, "*.xml")):
        r = ET.parse(x).getroot()
        t += int(r.get("tests", 0)); f += int(r.get("failures", 0))
        e += int(r.get("errors", 0)); s += int(r.get("skipped", 0))
    if t == 0:
        print(f"{os.path.basename(d):45s} NO RESULTS - the class did not run, check its log")
        continue
    print(f"{os.path.basename(d):45s} tests={t:4d} passed={t-f-e-s:4d} "
          f"failed={f:3d} errors={e:3d} skipped={s:4d}")
EOF
