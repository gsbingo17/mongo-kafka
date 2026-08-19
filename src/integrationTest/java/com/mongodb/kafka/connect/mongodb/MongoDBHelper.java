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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class MongoDBHelper
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {
  private static final String DEFAULT_URI = "mongodb://localhost:27017";
  private static final String URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
  private static final String DEFAULT_DATABASE_NAME = "MongoKafkaTest";
  // Modified: separate source and target endpoints for the round trip tests. Unset by default,
  // in which case both fall back to the main connection string.
  private static final String SOURCE_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.source.uri";
  private static final String TARGET_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.target.uri";

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBHelper.class);

  private ConnectionString connectionString;
  private MongoClient mongoClient;
  private ConnectionString sourceConnectionString;
  private MongoClient sourceMongoClient;
  private ConnectionString targetConnectionString;
  private MongoClient targetMongoClient;

  public MongoDBHelper() {}

  public MongoClient getMongoClient() {
    if (mongoClient == null) {
      mongoClient = MongoClients.create(getConnectionString());
    }
    return mongoClient;
  }

  @Override
  public void beforeAll(final ExtensionContext context) {
    getMongoClient();
  }

  /**
   * Modified: counts how many distinct databases a single test has asked for via {@link
   * MongoKafkaTestCase#getDatabaseWithPostfix()}. Reset before each test.
   */
  private static final AtomicInteger PINNED_DATABASE_HANDOUTS = new AtomicInteger();

  static int nextPinnedDatabaseHandout() {
    return PINNED_DATABASE_HANDOUTS.incrementAndGet();
  }

  @Override
  public void beforeEach(final ExtensionContext context) {
    PINNED_DATABASE_HANDOUTS.set(0);
    if (mongoClient != null) {
      // Modified: Firestore Enterprise rejects dropDatabase ("Unsupported command dropDatabase",
      // code 2 / InvalidArgument), so drop the collections instead.
      dropCollections(getDatabase());
    }
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    if (mongoClient != null) {
      // Modified: Skipped dropDatabase as Firestore Enterprise does not support it.
      dropCollections(getDatabase());
    }
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
    if (sourceMongoClient != null) {
      sourceMongoClient.close();
      sourceMongoClient = null;
    }
    if (targetMongoClient != null) {
      targetMongoClient.close();
      targetMongoClient = null;
    }
  }

  /**
   * Modified: stand-in for {@link MongoDatabase#drop()}, which Firestore Enterprise does not
   * support. Empties the database by dropping each collection in it.
   */
  public static void dropCollections(final MongoDatabase database) {
    for (String collectionName : database.listCollectionNames()) {
      try {
        database.getCollection(collectionName).drop();
      } catch (RuntimeException e) {
        LOGGER.warn("Unable to drop collection '{}': {}", collectionName, e.getMessage());
      }
    }
  }

  public MongoClient getSourceMongoClient() {
    if (sourceMongoClient == null) {
      sourceMongoClient = MongoClients.create(getSourceConnectionString());
    }
    return sourceMongoClient;
  }

  public MongoClient getTargetMongoClient() {
    if (targetMongoClient == null) {
      targetMongoClient = MongoClients.create(getTargetConnectionString());
    }
    return targetMongoClient;
  }

  public ConnectionString getSourceConnectionString() {
    if (sourceConnectionString == null) {
      sourceConnectionString = resolveConnectionString(SOURCE_URI_SYSTEM_PROPERTY_NAME);
    }
    return sourceConnectionString;
  }

  public ConnectionString getTargetConnectionString() {
    if (targetConnectionString == null) {
      targetConnectionString = resolveConnectionString(TARGET_URI_SYSTEM_PROPERTY_NAME);
    }
    return targetConnectionString;
  }

  public MongoDatabase getSourceDatabase() {
    String databaseName = getSourceConnectionString().getDatabase();
    return getSourceMongoClient()
        .getDatabase(databaseName != null ? databaseName : DEFAULT_DATABASE_NAME);
  }

  public MongoDatabase getTargetDatabase() {
    String databaseName = getTargetConnectionString().getDatabase();
    return getTargetMongoClient()
        .getDatabase(databaseName != null ? databaseName : DEFAULT_DATABASE_NAME);
  }

  private ConnectionString resolveConnectionString(final String systemPropertyName) {
    String uri = System.getProperty(systemPropertyName);
    if (uri == null || uri.isEmpty()) {
      return getConnectionString();
    }
    LOGGER.info("Connecting '{}' to: '{}'", systemPropertyName, new ConnectionString(uri));
    return new ConnectionString(uri);
  }

  public String getDatabaseName() {
    String databaseName = getConnectionString().getDatabase();
    return databaseName != null ? databaseName : DEFAULT_DATABASE_NAME;
  }

  public MongoDatabase getDatabase() {
    String databaseName = getConnectionString().getDatabase();
    return getMongoClient()
        .getDatabase(databaseName != null ? databaseName : DEFAULT_DATABASE_NAME);
  }

  public ConnectionString getConnectionString() {
    if (connectionString == null) {
      String mongoURIProperty = System.getProperty(URI_SYSTEM_PROPERTY_NAME);
      String mongoURIString =
          mongoURIProperty == null || mongoURIProperty.isEmpty() ? DEFAULT_URI : mongoURIProperty;
      connectionString = new ConnectionString(mongoURIString);
      LOGGER.info("Connecting to: '{}'", connectionString);
    }
    return connectionString;
  }
}
