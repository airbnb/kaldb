package com.slack.astra.graphApi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Header;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.slack.astra.zipkinApi.TraceFetcher;
import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
  APIs for exposing traces and their spans as subgraph dependencies.
*/
public class GraphService {
  private static final Logger LOG = LoggerFactory.getLogger(GraphService.class);
  private final TraceFetcher traceFetcher;
  private final GraphBuilder graphBuilder;

  private static final ObjectMapper objectMapper =
      JsonMapper.builder()
          // sort alphabetically for easier test asserts
          .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
          // don't serialize null values or empty maps
          .serializationInclusion(JsonInclude.Include.NON_EMPTY)
          .build();

  public GraphService(TraceFetcher traceFetcher, GraphConfig graphConfig) {
    this.traceFetcher = traceFetcher;
    this.graphBuilder = new GraphBuilder(graphConfig);

    LOG.info("Started GraphService with GraphBuilder config: {}", graphConfig);
  }

  private record SubgraphResponse(
      Graph subgraph, long traceFetchTimeMs, long subgraphBuildTimeMs) {}

  @Get
  @Path("/api/v1/trace/{traceId}/subgraph")
  public HttpResponse getSubgraph(
      @Param("traceId") String traceId,
      @Param("buildFilter") Optional<String> buildFilterJson,
      @Param("maxSpans") Optional<Integer> maxSpans,
      @Header("X-User-Request") Optional<Boolean> userRequest)
      throws IOException {
    // Parse the filter from JSON string if provided
    Optional<GraphBuilder.Filter> buildFilter = Optional.empty();
    if (buildFilterJson.isPresent()) {
      try {
        // Parse JSON as Map<String, List<String>> and create Filter
        TypeReference<Map<String, List<String>>> typeRef = new TypeReference<>() {};
        Map<String, List<String>> filterMap =
            objectMapper.readValue(buildFilterJson.get(), typeRef);
        buildFilter = Optional.of(new GraphBuilder.Filter(filterMap));
      } catch (Exception e) {
        LOG.error("Failed to parse buildFilter JSON: {}", buildFilterJson.get(), e);
        return HttpResponse.of(
            HttpStatus.BAD_REQUEST,
            MediaType.PLAIN_TEXT_UTF_8,
            "Invalid buildFilter JSON: " + e.getMessage());
      }
    }

    long start = System.currentTimeMillis();
    List<ZipkinSpanResponse> trace =
        this.traceFetcher.getSpansByTraceId(
            traceId, Optional.empty(), Optional.empty(), maxSpans, userRequest, Optional.empty());
    long end = System.currentTimeMillis();
    long traceFetchTime = end - start;

    start = System.currentTimeMillis();
    Graph subgraph = this.graphBuilder.buildFromSpans(trace, buildFilter);
    end = System.currentTimeMillis();
    long subgraphBuildTime = end - start;

    SubgraphResponse response = new SubgraphResponse(subgraph, traceFetchTime, subgraphBuildTime);
    String output = objectMapper.writeValueAsString(response);
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }
}
