package com.slack.astra.logstore.duckdb;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.google.common.io.Files;
import com.slack.astra.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DuckdbIndexStoreImplTest {
  private SimpleMeterRegistry meterRegistry;
  private DuckdbIndexStoreImpl logStore;
  private File tempFolder;

  @BeforeAll
  public static void beforeClass() {
    Tracing.newBuilder().build();
  }

  @BeforeEach
  public void startup() throws SQLException {
    meterRegistry = new SimpleMeterRegistry();
    tempFolder = Files.createTempDir();
    logStore = new DuckdbIndexStoreImpl(tempFolder, meterRegistry);
  }

  @AfterEach
  public void shutdown() throws IOException, TimeoutException {
    logStore.close();
    meterRegistry.close();
    FileUtils.deleteDirectory(tempFolder);
  }

  public static void addMessages(
      DuckdbIndexStoreImpl logStore, int low, int high, boolean requireCommit) {

    for (Trace.Span m : SpanUtil.makeSpansWithTimeDifference(low, high, 1, Instant.now())) {
      logStore.addMessage(m);
    }
    if (requireCommit) {
      logStore.commit();
      logStore.refresh();
    }
  }

  @Test
  public void testDuckdbLogStoreImpl() throws SQLException {
    addMessages(logStore, 1, 100, true);
    logStore.commit();
    assertThat(countInsertedRows(logStore)).isEqualTo(100);
    assertThat(findMessagesWithId(logStore, "Message1")).isEqualTo(1);
    assertThat(findMessagesWithId(logStore, "Message100")).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, logStore.registry)).isEqualTo(100);
    //        assertThat(getCount(MESSAGES_FAILED_COUNTER, logStore.metricsRegistry)).isEqualTo(0);
    //        assertThat(getTimerCount(REFRESHES_TIMER, logStore.metricsRegistry)).isEqualTo(1);
    //        assertThat(getTimerCount(COMMITS_TIMER, logStore.metricsRegistry)).isEqualTo(1);
  }

  private int countInsertedRows(DuckdbIndexStoreImpl duckdbIndexStore) throws SQLException {
    ResultSet countResult = duckdbIndexStore.executeSQLQuery("SELECT COUNT(*) from spans");
    int count = 0;
    while (countResult.next()) {
      count = Integer.valueOf(countResult.getString(1));
    }
    return count;
  }

  private int findMessagesWithId(DuckdbIndexStoreImpl duckdbIndexStore, String messageId)
      throws SQLException {
    String selectQuery = String.format("SELECT COUNT(*) from spans WHERE id = '%s'", messageId);
    ResultSet countResult = duckdbIndexStore.executeSQLQuery(selectQuery);
    int count = 0;
    while (countResult.next()) {
      count = Integer.valueOf(countResult.getString(1));
    }
    return count;
  }
}
