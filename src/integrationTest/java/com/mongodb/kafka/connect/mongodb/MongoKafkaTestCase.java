/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kafka.connect.mongodb;

import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.ChangeStreamOperation;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.confluent.connect.avro.AvroConverter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;

import com.mongodb.kafka.connect.MongoSinkConnector;
import com.mongodb.kafka.connect.MongoSourceConnector;
import com.mongodb.kafka.connect.embedded.EmbeddedKafka;
import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;

public class MongoKafkaTestCase {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MongoKafkaTestCase.class);
  protected static final AtomicInteger POSTFIX = new AtomicInteger();
  private static final int DEFAULT_MAX_RETRIES = 15;
  private static final int DEFAULT_EMPTY_RETRIES = 5;
  private static final OutputFormat DEFAULT_OUTPUT_FORMAT = OutputFormat.JSON;
  // Modified: Firestore Enterprise test properties. All unset by default.
  private static final String DATABASE_SYSTEM_PROPERTY_NAME = "org.mongodb.test.database";
  private static final String SKIP_COLLECTION_CHANGE_STREAMS_PROPERTY_NAME =
      "org.mongodb.test.skip.collection.change.streams";
  private static final String FORCE_DATABASE_SCOPE_PROPERTY_NAME =
      "org.mongodb.test.force.database.scope";

  @RegisterExtension public static final EmbeddedKafka KAFKA = new EmbeddedKafka();
  @RegisterExtension public static final MongoDBHelper MONGODB = new MongoDBHelper();

  /**
   * Modified: Firestore Enterprise requires a change stream to be provisioned out of band for each
   * scope it is watched at. Watching a collection whose collection group has no provisioned stream
   * fails with "Collection group scope change stream is not active for this collection group" (code
   * 2 / InvalidArgument). That is a provisioning boundary rather than a connector incompatibility.
   *
   * <p>The collection group does <em>not</em> have to exist when the stream is provisioned, so this
   * is not an inherent barrier. Two parameters decide what happens for a test that watches at
   * collection scope:
   *
   * <ul>
   *   <li><b>do</b> - set {@code org.mongodb.test.firestore.project} and {@link
   *       FirestoreChangeStreamProvisioner} creates the stream for that collection group and waits
   *       for it to activate, so the test runs for real.
   *   <li><b>skip</b> - leave that unset and set {@code
   *       org.mongodb.test.skip.collection.change.streams=true}, for endpoints where provisioning
   *       is not available. The test reports as SKIPPED with the reason attached; it is never
   *       counted as a pass.
   * </ul>
   *
   * <p>With neither set the test runs and fails on the unprovisioned stream, which is the honest
   * default for an endpoint nothing is known about.
   */
  public static void prepareCollectionScopeChangeStream(
      final String databaseName, final String collectionName) {
    if (collectionName == null || collectionName.isEmpty()) {
      return;
    }
    if (FirestoreChangeStreamProvisioner.isEnabled()) {
      FirestoreChangeStreamProvisioner.ensureCollectionGroupStream(databaseName, collectionName);
      return;
    }
    skipIfCollectionScopeChangeStream(collectionName);
  }

  /**
   * Modified: the skip half of {@link #prepareCollectionScopeChangeStream(String, String)}, for the
   * one call site that has to abort before the collection it watches is known. Inert when
   * provisioning is enabled, so it cannot skip a test the provisioner would have let run.
   */
  public static void skipIfCollectionScopeChangeStream(final String collectionName) {
    if (collectionName == null
        || collectionName.isEmpty()
        || FirestoreChangeStreamProvisioner.isEnabled()) {
      return;
    }
    assumeFalse(
        Boolean.parseBoolean(System.getProperty(SKIP_COLLECTION_CHANGE_STREAMS_PROPERTY_NAME)),
        format(
            "Skipping collection scope change stream on '%s': no change stream has been provisioned"
                + " for this collection group on the endpoint under test.",
            collectionName));
  }

  public MongoDatabase getSourceDatabase() {
    return MONGODB.getSourceDatabase();
  }

  public MongoDatabase getTargetDatabase() {
    return MONGODB.getTargetDatabase();
  }

  public String getSourceConnectionUri() {
    return MONGODB.getSourceConnectionString().toString();
  }

  public String getTargetConnectionUri() {
    return MONGODB.getTargetConnectionString().toString();
  }

  /**
   * Modified: the target endpoint may be a pre-existing database holding data that is not ours, so
   * it is never swept wholesale - only the specific collections a test creates there are dropped.
   */
  public void dropTargetCollections(final String... collectionNames) {
    for (String collectionName : collectionNames) {
      try {
        getTargetDatabase().getCollection(collectionName).drop();
      } catch (RuntimeException e) {
        LOGGER.warn("Unable to drop target collection '{}': {}", collectionName, e.getMessage());
      }
    }
  }

  /**
   * Modified: counterpart to {@link #dropTargetCollections(String...)}. {@link #cleanUp()} sweeps
   * the main endpoint ({@code org.mongodb.test.uri}), which only clears the source when the source
   * URI happens to point at it. The round trip classes take source and target as independent
   * endpoints, so they clear their own source collections through here.
   */
  public void dropSourceCollections(final String... collectionNames) {
    for (String collectionName : collectionNames) {
      try {
        getSourceDatabase().getCollection(collectionName).drop();
      } catch (RuntimeException e) {
        LOGGER.warn("Unable to drop source collection '{}': {}", collectionName, e.getMessage());
      }
    }
  }

  public String getTopicName() {
    return format("%s%s", getCollectionName(), POSTFIX.incrementAndGet());
  }

  public MongoClient getMongoClient() {
    return MONGODB.getMongoClient();
  }

  public String getDatabaseName() {
    return MONGODB.getDatabaseName();
  }

  public MongoDatabase getDatabase() {
    return MONGODB.getDatabase();
  }

  public String getCollectionName() {
    String collection = MONGODB.getConnectionString().getCollection();
    return collection != null ? collection : getClass().getSimpleName();
  }

  public MongoCollection<Document> getCollection() {
    return getCollection(getCollectionName());
  }

  public MongoCollection<Document> getCollection(final String name) {
    return MONGODB.getDatabase().getCollection(name);
  }

  public boolean isReplicaSetOrSharded() {
    Document isMaster =
        MONGODB
            .getMongoClient()
            .getDatabase("admin")
            .runCommand(BsonDocument.parse("{isMaster: 1}"));
    return isMaster.containsKey("setName") || isMaster.get("msg", "").equals("isdbgrid");
  }

  private static final int THREE_DOT_SIX_WIRE_VERSION = 6;
  private static final int FOUR_DOT_ZERO_WIRE_VERSION = 7;
  private static final int FOUR_DOT_TWO_WIRE_VERSION = 8;
  public static final int FOUR_DOT_FOUR_WIRE_VERSION = 9;
  private static final int SIX_DOT_ZERO_WIRE_VERSION = 17;
  private static final int SEVEN_DOT_ZERO_WIRE_VERSION = 21;

  public boolean isGreaterThanThreeDotSix() {
    return getMaxWireVersion() > THREE_DOT_SIX_WIRE_VERSION;
  }

  public boolean isGreaterThanFourDotZero() {
    return getMaxWireVersion() > FOUR_DOT_ZERO_WIRE_VERSION;
  }

  public boolean isGreaterThanFourDotTwo() {
    return getMaxWireVersion() > FOUR_DOT_TWO_WIRE_VERSION;
  }

  public boolean isGreaterThanFourDotFour() {
    return getMaxWireVersion() > FOUR_DOT_FOUR_WIRE_VERSION;
  }

  public boolean isAtLeastSixDotZero() {
    return getMaxWireVersion() >= SIX_DOT_ZERO_WIRE_VERSION;
  }

  public boolean isAtLeastSevenDotZero() {
    return getMaxWireVersion() >= SEVEN_DOT_ZERO_WIRE_VERSION;
  }

  /**
   * Modified: no longer queries the server. Firestore Enterprise reports {@code maxWireVersion:
   * 16}, but it is not a MongoDB release, so that number does not place it on the version line and
   * the version gates built on it decide nothing useful about what Firestore supports - they only
   * skip tests before they can produce a compatibility signal. Returning the highest version opens
   * every gate so the tests actually run. {@link #isReplicaSetOrSharded()} still queries the server
   * and is deliberately kept, as it tests a real change stream precondition.
   */
  public int getMaxWireVersion() {
    return SEVEN_DOT_ZERO_WIRE_VERSION;
  }

  public void cleanUp() {
    KAFKA.resetOffsets();
    // Modified: Firestore Enterprise does not support dropDatabase, so empty each matching
    // database by dropping its collections instead.
    getMongoClient()
        .listDatabaseNames()
        .into(new ArrayList<>())
        .forEach(
            i -> {
              if (i.startsWith(getDatabaseName())) {
                MongoDBHelper.dropCollections(getMongoClient().getDatabase(i));
              }
            });
    getPinnedDatabaseName()
        .ifPresent(name -> MongoDBHelper.dropCollections(getMongoClient().getDatabase(name)));
    // Clean up any stale JMX MBeans from previous test runs
    cleanUpMBeans();
  }

  private void cleanUpMBeans() {
    try {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      Set<ObjectName> mbeans =
          mBeanServer.queryNames(new ObjectName("com.mongodb.kafka.connect:*"), null);
      for (ObjectName mbean : mbeans) {
        try {
          mBeanServer.unregisterMBean(mbean);
        } catch (Exception e) {
          // Ignore - MBean might already be unregistered
        }
      }
    } catch (Exception e) {
      // Ignore - JMX might not be available
    }
  }

  /**
   * Modified: databases cannot be created on demand on Firestore Enterprise - an unprovisioned name
   * is rejected with "Invalid database name" - so {@code org.mongodb.test.database} pins the ad-hoc
   * {@code <name><postfix>} databases to one database that already exists. Unset by default, which
   * keeps the original behaviour.
   *
   * <p>A test that calls this more than once wants that many <em>distinct</em> databases. Pinning
   * cannot provide them: the second call would hand back the first database, silently collapsing
   * two namespaces into one, and the test would then fail with a duplicate key error that looks
   * like a connector incompatibility but is really an artefact of this workaround. Skip instead, so
   * the test is reported as untested rather than as a spurious failure.
   */
  public MongoDatabase getDatabaseWithPostfix() {
    Optional<String> pinned = getPinnedDatabaseName();
    if (!pinned.isPresent()) {
      return getMongoClient()
          .getDatabase(format("%s%s", getDatabaseName(), POSTFIX.incrementAndGet()));
    }
    assumeFalse(
        MongoDBHelper.nextPinnedDatabaseHandout() > 1,
        "Skipping: this test needs more than one distinct database, and the endpoint cannot create"
            + " databases on demand, so org.mongodb.test.database pins them all to one.");
    return getMongoClient().getDatabase(pinned.get());
  }

  public MongoCollection<Document> getAndCreateCollection() {
    // Modified: this wants a distinct *collection*, not a distinct database, so it goes straight to
    // the pinned database rather than through getDatabaseWithPostfix(). When pinned, every call
    // returns the same database, so postfix the collection instead - otherwise the second call in a
    // test fails with NamespaceExists.
    Optional<String> pinned = getPinnedDatabaseName();
    MongoDatabase database =
        pinned
            .map(name -> getMongoClient().getDatabase(name))
            .orElseGet(
                () ->
                    getMongoClient()
                        .getDatabase(format("%s%s", getDatabaseName(), POSTFIX.incrementAndGet())));
    String collectionName =
        pinned.isPresent() ? format("coll%s", POSTFIX.incrementAndGet()) : "coll";
    database.createCollection(collectionName);
    return database.getCollection(collectionName);
  }

  protected static Optional<String> getPinnedDatabaseName() {
    String databaseName = System.getProperty(DATABASE_SYSTEM_PROPERTY_NAME);
    return databaseName == null || databaseName.isEmpty()
        ? Optional.empty()
        : Optional.of(databaseName);
  }

  /**
   * Modified: a source connector configured with neither {@code database} nor {@code collection}
   * watches the whole deployment. Firestore Enterprise has no such scope - {@code
   * MongoClient.watch()} is an aggregate against {@code admin}, and Firestore answers "unsupported
   * database `admin`" - because a Firestore database is a standalone endpoint rather than one
   * database inside a deployment. Deployment scope is therefore out of scope for this exercise.
   *
   * <p>Setting {@code org.mongodb.test.force.database.scope=true} supplies the source database that
   * such a connector would otherwise leave unset, turning the watch into a database scope one.
   *
   * <p>Five tests write everything they assert on into a single database and only watch at
   * deployment scope incidentally ({@code testFullDocumentBeforeChange}, the three {@code
   * disambiguatedPaths} tests and {@code testTruncatedArrays}). For those the narrowed stream
   * carries the identical event set, so the features they exercise - pre-/post-images, {@code
   * showExpandedEvents}, truncated arrays - get a real verdict instead of failing on the scope.
   * Every payload assertion is untouched, so an unsupported feature still fails.
   *
   * <p>The five tests that genuinely span databases are unaffected: they abort earlier, on the
   * second {@link #getDatabaseWithPostfix()} handout.
   *
   * <p>Returns null when the property is unset, which leaves the original behaviour in place.
   */
  protected static String forcedSourceDatabase() {
    if (!Boolean.parseBoolean(System.getProperty(FORCE_DATABASE_SCOPE_PROPERTY_NAME))) {
      return null;
    }
    return getPinnedDatabaseName().orElseGet(MONGODB::getDatabaseName);
  }

  private static final String SIMPLE_DOCUMENT = "{_id: %s}";

  public List<Document> createDocuments(final IntStream stream) {
    return createDocuments(stream, SIMPLE_DOCUMENT);
  }

  public List<Document> createDocuments(final IntStream stream, final String json) {
    return stream.mapToObj(i -> Document.parse(format(json, i))).collect(toList());
  }

  public List<Document> insertMany(
      final IntStream stream, final MongoCollection<?>... collections) {
    return insertMany(stream, SIMPLE_DOCUMENT, collections);
  }

  public List<Document> insertMany(
      final IntStream stream, final String json, final MongoCollection<?>... collections) {
    List<Document> docs = createDocuments(stream, json);
    for (MongoCollection<?> c : collections) {
      LOGGER.debug("Inserting {} documents into {} ", docs.size(), c.getNamespace().getFullName());
      c.withDocumentClass(Document.class).insertMany(docs);
    }
    return docs;
  }

  public <T> void assertDatabase(final MongoDatabase original, final MongoDatabase destination) {
    LOGGER.info("Asserting database match {} : {}", original.getName(), destination.getName());

    List<String> originalCollections =
        original.listCollectionNames().into(new ArrayList<>()).stream().sorted().collect(toList());

    retry(
        () -> {
          List<String> destinationCollections =
              destination.listCollectionNames().into(new ArrayList<>());
          return !destinationCollections.isEmpty()
              && destinationCollections.size() == originalCollections.size();
        },
        () ->
            assertIterableEquals(
                originalCollections,
                destination.listCollectionNames().into(new ArrayList<>()).stream()
                    .sorted()
                    .collect(toList())));

    originalCollections.forEach(
        collectionName ->
            assertCollection(
                original.getCollection(collectionName), destination.getCollection(collectionName)));
  }

  public <T> void assertCollection(
      final MongoCollection<T> original, final MongoCollection<T> destination) {
    LOGGER.info(
        "Asserting collections match {} : {}",
        original.getNamespace().getFullName(),
        destination.getNamespace().getFullName());

    assertCollection(
        original.find().sort(Sorts.ascending("_id")).into(new ArrayList<>()), destination);
  }

  public <T> void assertCollection(final List<T> expected, final MongoCollection<T> destination) {
    retry(
        () -> destination.countDocuments() == expected.size(),
        () ->
            assertIterableEquals(
                expected, destination.find().sort(Sorts.ascending("_id")).into(new ArrayList<>())));
  }

  public void retry(final Supplier<Boolean> check, final Runnable assertion) {
    int retry = 0;
    while (retry < DEFAULT_MAX_RETRIES) {
      retry++;
      if (!check.get()) {
        // Exponentially back off when retrying (max wait time is ~5mins)
        sleep(1000 + (250 * (long) Math.pow(retry, 2)));
      } else {
        assertion.run();
        return;
      }
    }
    assertion.run();
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes, final MongoCollection<?> coll) {
    assertProduced(
        operationTypes,
        coll,
        operationTypes.isEmpty() ? DEFAULT_EMPTY_RETRIES : DEFAULT_MAX_RETRIES,
        DEFAULT_OUTPUT_FORMAT);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes,
      final MongoCollection<?> coll,
      final OutputFormat outputFormat) {
    assertProduced(
        operationTypes,
        coll,
        operationTypes.isEmpty() ? DEFAULT_EMPTY_RETRIES : DEFAULT_MAX_RETRIES,
        outputFormat);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes,
      final MongoCollection<?> coll,
      final int maxRetryCount,
      final OutputFormat outputFormat) {
    assertProduced(operationTypes, coll.getNamespace().getFullName(), maxRetryCount, outputFormat);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes, final String topicName) {
    assertProduced(operationTypes, topicName, DEFAULT_MAX_RETRIES);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes,
      final String topicName,
      final int maxRetryCount) {
    assertProduced(operationTypes, topicName, maxRetryCount, DEFAULT_OUTPUT_FORMAT);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes,
      final String topicName,
      final int maxRetryCount,
      final OutputFormat outputFormat) {

    List<ChangeStreamOperation> produced;
    switch (outputFormat) {
      case JSON:
        produced =
            getProduced(
                topicName,
                ChangeStreamOperations::createChangeStreamOperationJson,
                operationTypes,
                maxRetryCount);
        break;
      case BSON:
        produced =
            getProduced(
                topicName,
                ChangeStreamOperations::createChangeStreamOperationBson,
                operationTypes,
                maxRetryCount);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + outputFormat);
    }
    assertIterableEquals(operationTypes, produced);
  }

  public void assertProducedDocs(final List<Document> docs, final MongoCollection<?> coll) {
    List<Document> produced =
        getProduced(
            coll.getNamespace().getFullName(),
            b -> Document.parse(b.toString()),
            docs,
            DEFAULT_MAX_RETRIES);
    assertIterableEquals(docs, produced);
  }

  private static final Deserializer<Bytes> BYTES_DESERIALIZER = new BytesDeserializer();

  public BsonDocument getHeartbeat(final String topicName) {
    return getProduced(
            topicName,
            new JsonDeserializer(),
            new JsonDeserializer(),
            c ->
                BsonDocument.parse(
                    format("{key: %s, value: %s}", c.key().textValue(), c.value().textValue())),
            1,
            1)
        .get(0);
  }

  public List<String> getProducedStrings(final String topicName, final int expectedSize) {
    return getProduced(
        topicName,
        BYTES_DESERIALIZER,
        new MappingDeserializer<>(Bytes::toString),
        ConsumerRecord::value,
        expectedSize,
        1);
  }

  public <T> List<T> getProduced(
      final String topicName,
      final Function<Bytes, T> mapper,
      final List<T> expected,
      final int maxRetryCount) {
    return getProduced(
        topicName,
        BYTES_DESERIALIZER,
        new MappingDeserializer<>(mapper),
        ConsumerRecord::value,
        expected,
        maxRetryCount);
  }

  public static class MappingDeserializer<T> implements Deserializer<T> {
    private final Function<Bytes, T> mapper;

    public MappingDeserializer(final Function<Bytes, T> mapper) {
      this.mapper = mapper;
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
      return mapper.apply(BYTES_DESERIALIZER.deserialize(topic, data));
    }
  }

  public <K, V, T> List<T> getProduced(
      final String topicName,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer,
      final Function<ConsumerRecord<K, V>, T> mapper,
      final int expectedSize,
      final int maxRetryCount) {
    return getProduced(
        topicName,
        keyDeserializer,
        valueDeserializer,
        mapper,
        emptyList(),
        expectedSize,
        maxRetryCount);
  }

  public <K, V, T> List<T> getProduced(
      final String topicName,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer,
      final Function<ConsumerRecord<K, V>, T> mapper,
      final List<T> expected,
      final int maxRetryCount) {
    return getProduced(
        topicName,
        keyDeserializer,
        valueDeserializer,
        mapper,
        expected,
        expected.size(),
        maxRetryCount);
  }

  public <K, V, T> List<T> getProduced(
      final String topicName,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer,
      final Function<ConsumerRecord<K, V>, T> mapper,
      final List<T> expected,
      final int expectedSize,
      final int maxRetryCount) {
    LOGGER.info("Subscribing to {}", topicName);
    if (!expected.isEmpty() && expected.size() != expectedSize) {
      throw new IllegalArgumentException("Expected list size different from expected size");
    }

    try (KafkaConsumer<K, V> consumer = createConsumer(keyDeserializer, valueDeserializer)) {
      consumer.subscribe(singletonList(topicName));
      List<T> data = new ArrayList<>();
      int counter = 0;
      int retryCount = 0;
      int previousDataSize;

      while (retryCount < maxRetryCount) {
        counter++;
        previousDataSize = data.size();
        consumer
            .poll(Duration.ofSeconds(2))
            .records(topicName)
            .forEach(c -> data.add(mapper.apply(c)));

        if (data.size() >= expectedSize) {
          int startIndex = expected.isEmpty() ? 0 : Collections.indexOfSubList(data, expected);
          if (startIndex > -1) {
            return data.subList(startIndex, startIndex + expectedSize);
          }
        }

        // Wait at least 3 minutes for the first set of data to arrive
        if (expectedSize == 0 || data.size() > 0 || counter > 90) {
          retryCount += previousDataSize == data.size() ? 1 : 0;
        }
      }

      return data;
    }
  }

  public KafkaConsumer<?, ?> createConsumer() {
    return createConsumer(null, null);
  }

  public <K, V> KafkaConsumer<K, V> createConsumer(
      final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
    Properties props = new Properties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "testAssertProducedConsumer");
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.BytesDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.BytesDeserializer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    return new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
  }

  public void addSinkConnector(final String topicName) {
    Properties props = createSinkProperties();
    props.put("topics", topicName);
    addSinkConnector(props);
  }

  public Properties createSinkProperties() {
    Properties props = new Properties();
    props.put("connector.class", MongoSinkConnector.class.getName());
    props.put(MongoSinkConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
    props.put(MongoSinkTopicConfig.DATABASE_CONFIG, MONGODB.getDatabaseName());
    props.put(MongoSinkTopicConfig.COLLECTION_CONFIG, getCollectionName());
    props.put("key.converter", AvroConverter.class.getName());
    props.put("key.converter.schema.registry.url", KAFKA.schemaRegistryUrl());
    props.put("value.converter", AvroConverter.class.getName());
    props.put("value.converter.schema.registry.url", KAFKA.schemaRegistryUrl());
    return props;
  }

  public void addSinkConnector(final Properties sinkProperties) {
    KAFKA.addSinkConnector(sinkProperties);
  }

  public void addSourceConnector() {
    addSourceConnector(new Properties());
  }

  public void addSourceConnector(final Properties overrides) {
    Properties props = new Properties();
    props.put("connector.class", MongoSourceConnector.class.getName());
    props.put(MongoSourceConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
    // Modified: supply a source database when the test leaves one unset, so the connector watches
    // at database scope rather than at the deployment scope Firestore does not have. Inert unless
    // org.mongodb.test.force.database.scope is set, and an explicit override always wins.
    String forcedDatabase = forcedSourceDatabase();
    if (forcedDatabase != null) {
      props.put(MongoSourceConfig.DATABASE_CONFIG, forcedDatabase);
    }

    overrides.forEach(props::put);

    // Modified: the check lives here, where a source change stream is opened, rather than in the
    // individual tests, so no test side logic changes and the skip cannot be forgotten on a new
    // test. It reads the merged properties so it sees the scope the connector will actually use.
    prepareCollectionScopeChangeStream(
        props.getProperty(MongoSourceConfig.DATABASE_CONFIG, MONGODB.getDatabaseName()),
        props.getProperty(MongoSourceConfig.COLLECTION_CONFIG));

    KAFKA.addSourceConnector(props);
  }

  public void restartSinkConnector() {
    KAFKA.restartSinkConnector();
  }

  public void restartSourceConnector() {
    KAFKA.restartSourceConnector();
  }

  public void stopStartSourceConnector(final Properties properties) {
    KAFKA.deleteSourceConnector();
    addSourceConnector(properties);
  }

  public void sleep() {
    sleep(2000);
  }

  public void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      // Ignore
    }
  }
}
