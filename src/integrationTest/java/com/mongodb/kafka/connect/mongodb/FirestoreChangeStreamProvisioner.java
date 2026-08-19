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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Modified: provisions Firestore Enterprise collection-group change streams so that collection
 * scope tests can run.
 *
 * <p>Firestore will not open a collection scope change stream unless one has been provisioned out
 * of band for that collection group; watching an unprovisioned collection fails with "Collection
 * group scope change stream is not active for this collection group" (code 2 / InvalidArgument).
 * The collection group itself does not have to exist yet, so a stream can be provisioned for a
 * collection the test has not created, which is what makes this automatable.
 *
 * <p>This is setup, in the same category as creating a database: it removes a precondition that
 * stops a test reaching connector logic. It cannot mask a Firestore incompatibility, because it
 * only makes the change stream available - everything the test then asserts about the connector is
 * unchanged.
 *
 * <p>Enabled by setting {@code org.mongodb.test.firestore.project} to the Google Cloud project id.
 * When it is unset this class is inert and the harness falls back to {@code
 * org.mongodb.test.skip.collection.change.streams}. Provisioning goes through the Firestore Admin
 * API; the access token comes from {@code gcloud auth print-access-token}.
 *
 * <p>Newly created streams take a few minutes to become active, so each one is polled until its
 * {@code startTime} has passed, plus a settle margin. Results are cached per JVM and creation is
 * idempotent, so the wait is paid once per collection group rather than once per test.
 */
public final class FirestoreChangeStreamProvisioner {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FirestoreChangeStreamProvisioner.class);

  private static final String PROJECT_PROPERTY_NAME = "org.mongodb.test.firestore.project";
  private static final String RETENTION_PROPERTY_NAME = "org.mongodb.test.firestore.retention";
  private static final String DEFAULT_RETENTION = "86400s";
  private static final String API_ROOT = "https://firestore.googleapis.com/v1";
  private static final String CHANGE_STREAM_ID_PREFIX = "kafka-it-";

  private static final long ACTIVATION_TIMEOUT_MS = 10 * 60 * 1000L;
  private static final long POLL_INTERVAL_MS = 15 * 1000L;

  /**
   * A stream is not reliably capturing writes the instant its {@code startTime} passes, and a test
   * that returns from here goes straight on to write. Settle past the boundary so a write cannot
   * land in the gap and be missed.
   */
  private static final long ACTIVATION_SETTLE_MS = 15 * 1000L;

  /**
   * How long a fetched access token is reused before it is re-minted. This is an optimisation, not
   * the correctness mechanism: gcloud returns a token from its own cache whose remaining life
   * cannot be predicted from when we fetched it, so the retry in {@link #request} is what actually
   * keeps a long run from failing on HTTP 401. A TTL longer than the token's own lifetime therefore
   * costs at most one extra round trip per expiry, not a failed run.
   */
  private static final long ACCESS_TOKEN_TTL_MS = 120 * 60 * 1000L;

  private static final Set<String> ENSURED = ConcurrentHashMap.newKeySet();
  private static volatile String cachedAccessToken;
  private static volatile long cachedAccessTokenExpiresAt;

  private FirestoreChangeStreamProvisioner() {}

  /** Whether change stream provisioning is configured for this run. */
  public static boolean isEnabled() {
    return project() != null;
  }

  /**
   * Ensures an active collection-group change stream exists for {@code collectionGroup}, creating
   * it and waiting for it to activate if necessary. Idempotent and cached per JVM.
   */
  public static void ensureCollectionGroupStream(
      final String databaseId, final String collectionGroup) {
    String projectId = project();
    if (projectId == null || databaseId == null || collectionGroup == null) {
      return;
    }
    String key = databaseId + "/" + collectionGroup;
    if (ENSURED.contains(key)) {
      return;
    }
    synchronized (FirestoreChangeStreamProvisioner.class) {
      if (ENSURED.contains(key)) {
        return;
      }
      Instant startTime = findStartTime(projectId, databaseId, collectionGroup);
      if (startTime == null) {
        LOGGER.info(
            "Provisioning collection group change stream for '{}' on database '{}'",
            collectionGroup,
            databaseId);
        startTime = create(projectId, databaseId, collectionGroup);
      }
      awaitActive(projectId, databaseId, collectionGroup, startTime);
      ENSURED.add(key);
    }
  }

  private static Instant findStartTime(
      final String projectId, final String databaseId, final String collectionGroup) {
    BsonDocument response =
        BsonDocument.parse(request("GET", changeStreamsUrl(projectId, databaseId), null));
    if (!response.containsKey("changeStreams")) {
      return null;
    }
    for (BsonValue value : response.getArray("changeStreams")) {
      BsonDocument changeStream = value.asDocument();
      BsonDocument scope = changeStream.getDocument("collectionGroupScope", new BsonDocument());
      if (scope.containsKey("collectionGroupId")
          && collectionGroup.equals(scope.getString("collectionGroupId").getValue())) {
        return startTimeOf(changeStream);
      }
    }
    return null;
  }

  private static Instant create(
      final String projectId, final String databaseId, final String collectionGroup) {
    String url =
        format(
            "%s?changeStreamId=%s",
            changeStreamsUrl(projectId, databaseId), changeStreamId(collectionGroup));
    String body =
        format(
            "{\"collectionGroupScope\":{\"collectionGroupId\":\"%s\"},\"retentionPeriod\":\"%s\"}",
            collectionGroup, retention());
    Instant startTime = startTimeOf(BsonDocument.parse(request("POST", url, body)));
    if (startTime == null) {
      // A concurrent run may have created it first; fall back to the listed stream.
      startTime = findStartTime(projectId, databaseId, collectionGroup);
    }
    return startTime;
  }

  private static void awaitActive(
      final String projectId,
      final String databaseId,
      final String collectionGroup,
      final Instant startTime) {
    if (startTime == null) {
      throw new IllegalStateException(
          format(
              "Could not determine the activation time of the change stream for collection group"
                  + " '%s' on database '%s'",
              collectionGroup, databaseId));
    }
    Instant settled = startTime.plusMillis(ACTIVATION_SETTLE_MS);
    long deadline = System.currentTimeMillis() + ACTIVATION_TIMEOUT_MS;
    while (Instant.now().isBefore(settled)) {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException(
            format(
                "Change stream for collection group '%s' on database '%s' was still not active at"
                    + " its start time of %s",
                collectionGroup, databaseId, startTime));
      }
      LOGGER.info(
          "Waiting for the change stream on collection group '{}' to become active at {}",
          collectionGroup,
          startTime);
      sleep(POLL_INTERVAL_MS);
    }
  }

  private static String changeStreamsUrl(final String projectId, final String databaseId) {
    return format("%s/projects/%s/databases/%s/changeStreams", API_ROOT, projectId, databaseId);
  }

  /** Change stream ids must start with a lowercase letter and hold only lowercase, digits, "-". */
  private static String changeStreamId(final String collectionGroup) {
    return CHANGE_STREAM_ID_PREFIX
        + collectionGroup.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9-]", "-");
  }

  private static Instant startTimeOf(final BsonDocument changeStream) {
    return changeStream.containsKey("startTime")
        ? Instant.parse(changeStream.getString("startTime").getValue())
        : null;
  }

  /**
   * {@link #ACCESS_TOKEN_TTL_MS} is measured from when the token was fetched, but {@code gcloud
   * auth print-access-token} returns a token from its own cache that may already be close to
   * expiry, so the TTL alone cannot rule out a 401 mid-run. Discard the cached token and retry once
   * on the first one. A second 401 is a real authentication problem, not an aged token, so it is
   * allowed to fail the run.
   */
  private static String request(final String method, final String url, final String body) {
    try {
      return send(method, url, body);
    } catch (UnauthorizedException e) {
      LOGGER.info("Access token rejected; re-minting and retrying {} {}", method, url);
      synchronized (FirestoreChangeStreamProvisioner.class) {
        cachedAccessToken = null;
        cachedAccessTokenExpiresAt = 0;
      }
      try {
        return send(method, url, body);
      } catch (UnauthorizedException retried) {
        throw new IllegalStateException(retried.getMessage(), retried);
      }
    }
  }

  private static final class UnauthorizedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    UnauthorizedException(final String message) {
      super(message);
    }
  }

  private static String send(final String method, final String url, final String body) {
    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod(method);
      connection.setRequestProperty("Authorization", "Bearer " + accessToken());
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setConnectTimeout(30_000);
      connection.setReadTimeout(60_000);
      if (body != null) {
        connection.setDoOutput(true);
        try (OutputStream out = connection.getOutputStream()) {
          out.write(body.getBytes(UTF_8));
        }
      }
      int status = connection.getResponseCode();
      InputStream stream =
          status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      String response = stream == null ? "" : read(stream);
      if (status == HttpURLConnection.HTTP_UNAUTHORIZED) {
        throw new UnauthorizedException(
            format("%s %s failed with HTTP %s: %s", method, url, status, response));
      }
      if (status >= 400) {
        throw new IllegalStateException(
            format("%s %s failed with HTTP %s: %s", method, url, status, response));
      }
      return response;
    } catch (IOException e) {
      throw new IllegalStateException(format("%s %s failed", method, url), e);
    }
  }

  private static String accessToken() {
    if (cachedAccessToken == null || System.currentTimeMillis() >= cachedAccessTokenExpiresAt) {
      synchronized (FirestoreChangeStreamProvisioner.class) {
        if (cachedAccessToken == null || System.currentTimeMillis() >= cachedAccessTokenExpiresAt) {
          cachedAccessToken = gcloudAccessToken();
          cachedAccessTokenExpiresAt = System.currentTimeMillis() + ACCESS_TOKEN_TTL_MS;
        }
      }
    }
    return cachedAccessToken;
  }

  private static String gcloudAccessToken() {
    ProcessBuilder builder = new ProcessBuilder("gcloud", "auth", "print-access-token");
    builder.redirectErrorStream(true);
    try {
      Process process = builder.start();
      String output;
      try (InputStream stream = process.getInputStream()) {
        output = read(stream).trim();
      }
      if (process.waitFor() != 0) {
        throw new IllegalStateException("'gcloud auth print-access-token' failed: " + output);
      }
      return output;
    } catch (IOException e) {
      throw new IllegalStateException("Unable to run 'gcloud auth print-access-token'", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted running 'gcloud auth print-access-token'", e);
    }
  }

  private static String read(final InputStream stream) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte[] chunk = new byte[4096];
    int read;
    while ((read = stream.read(chunk)) != -1) {
      buffer.write(chunk, 0, read);
    }
    return new String(buffer.toByteArray(), UTF_8);
  }

  private static void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for a change stream to activate", e);
    }
  }

  private static String project() {
    String projectId = System.getProperty(PROJECT_PROPERTY_NAME);
    return projectId == null || projectId.isEmpty() ? null : projectId;
  }

  private static String retention() {
    String retention = System.getProperty(RETENTION_PROPERTY_NAME);
    return retention == null || retention.isEmpty() ? DEFAULT_RETENTION : retention;
  }
}
