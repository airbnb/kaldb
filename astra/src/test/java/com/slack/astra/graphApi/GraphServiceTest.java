package com.slack.astra.graphApi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.slack.astra.zipkinApi.TraceFetcher;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GraphServiceTest {
  @Mock private TraceFetcher traceFetcher;
  private GraphService graphService;
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setup() throws IOException {
    MockitoAnnotations.openMocks(this);

    // Load custom config from YAML file
    Path configPath =
        new File(
                Objects.requireNonNull(
                        getClass()
                            .getClassLoader()
                            .getResource("test-dependency-graph-config.yaml"))
                    .getFile())
            .toPath();
    graphService = spy(new GraphService(traceFetcher, GraphConfig.load(configPath)));
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testGetSubgraphByTraceId_emptyResult() throws Exception {
    String traceId = "test_trace_empty";

    when(traceFetcher.getSpansByTraceId(
            anyString(),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class)))
        .thenReturn(Collections.emptyList());

    HttpResponse response = graphService.getSubgraph(traceId, Optional.empty());
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.OK, aggregatedResponse.status());

    // Verify it's valid JSON with nodes and edges arrays
    assertEquals("{}", aggregatedResponse.contentUtf8());
  }

  @Test
  public void testGetSubgraphByTraceId_withComplexHierarchy() throws Exception {
    String traceId = "test_trace_complex";

    // Create a complex span hierarchy:
    // root (API Gateway)
    //   ├── child1 (Auth Service)
    //   ├── child2 (User Service)
    //   │   ├── grandchild1 (Database)
    //   │   └── grandchild2 (Cache Service)
    //   └── child3 (Notification Service)
    //       └── grandchild3 (Email Service)
    List<com.slack.astra.zipkinApi.ZipkinSpanResponse> testSpans =
        List.of(
            // Root span - API Gateway
            TestUtils.createSpanWithTags(
                "root",
                traceId,
                null,
                Map.of(
                    "kube.app", "api-gateway",
                    "kube.namespace", "prod",
                    "operation_name", "http.request",
                    "resource", "/api/users/profile")),

            // First level children
            TestUtils.createSpanWithTags(
                "child1",
                traceId,
                "root",
                Map.of(
                    "kube.app", "auth-service",
                    "kube.namespace", "prod",
                    "operation_name", "http.request",
                    "resource", "/api/auth/validate")),
            TestUtils.createSpanWithTags(
                "child2",
                traceId,
                "root",
                Map.of(
                    "kube.app", "user-service",
                    "kube.namespace", "prod",
                    "operation_name", "user.fetch",
                    "resource", "getUserProfile")),
            TestUtils.createSpanWithTags(
                "child3",
                traceId,
                "root",
                Map.of(
                    "kube.app", "notification-service",
                    "kube.namespace", "prod",
                    "operation_name", "notify.send",
                    "resource", "sendNotification")),

            // Second level children (grandchildren)
            TestUtils.createSpanWithTags(
                "grandchild1",
                traceId,
                "child2",
                Map.of(
                    "kube.app", "postgres-db",
                    "kube.namespace", "prod",
                    "operation_name", "db.query",
                    "resource", "SELECT * FROM users WHERE id = ?")),
            TestUtils.createSpanWithTags(
                "grandchild2",
                traceId,
                "child2",
                Map.of(
                    "kube.app", "redis-cache",
                    "kube.namespace", "prod",
                    "operation_name", "cache.get",
                    "resource", "user:profile:12345")),
            TestUtils.createSpanWithTags(
                "grandchild3",
                traceId,
                "child3",
                Map.of(
                    "kube.app", "email-service",
                    "kube.namespace", "prod",
                    "operation_name", "email.send",
                    "resource", "sendEmail")));

    when(traceFetcher.getSpansByTraceId(
            anyString(),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class)))
        .thenReturn(testSpans);

    HttpResponse response = graphService.getSubgraph(traceId, Optional.empty());
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.OK, aggregatedResponse.status());

    // Verify it's valid JSON with proper structure
    String content = aggregatedResponse.contentUtf8();
    JsonNode jsonNode = objectMapper.readTree(content);

    // Verify structure
    assertTrue(jsonNode.has("nodes"));
    assertTrue(jsonNode.has("edges"));

    // Verify data content
    JsonNode nodes = jsonNode.get("nodes");
    JsonNode edges = jsonNode.get("edges");

    assertEquals(7, nodes.size(), "Should have 7 nodes (1 root + 3 children + 3 grandchildren)");
    assertEquals(
        6,
        edges.size(),
        "Should have 6 edges (3 root->child + 2 child2->grandchild + 1 child3->grandchild)");

    // Verify all nodes have required fields
    for (JsonNode node : nodes) {
      assertTrue(node.has("id"));
      assertTrue(node.has("metadata"));

      JsonNode metadata = node.get("metadata");
      assertTrue(metadata.has("app"));
      assertTrue(metadata.has("namespace"));
      assertTrue(metadata.has("resource"));
    }

    // Verify all edges have required fields
    for (JsonNode edge : edges) {
      assertTrue(edge.has("sourceNodeId"));
      assertTrue(edge.has("targetNodeId"));

      JsonNode metadata = edge.get("metadata");
      assertTrue(metadata.has("operation"));
    }
  }
}
