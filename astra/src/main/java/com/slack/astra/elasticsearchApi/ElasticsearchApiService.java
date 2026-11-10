package com.slack.astra.elasticsearchApi;

import brave.ScopedSpan;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.astra.elasticsearchApi.searchResponse.EsSearchResponse;
import com.slack.astra.elasticsearchApi.searchResponse.HitsMetadata;
import com.slack.astra.elasticsearchApi.searchResponse.SearchResponseHit;
import com.slack.astra.elasticsearchApi.searchResponse.SearchResponseMetadata;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.logstore.search.SearchResultUtils;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.StructuredTaskScope;
import java.util.regex.Pattern;
import org.jspecify.annotations.NonNull;
import org.opensearch.search.aggregations.InternalAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Elasticsearch compatible API service, for use in Grafana
 *
 * @see <a
 *     href="https://github.com/grafana/grafana/blob/main/public/app/plugins/datasource/elasticsearch/datasource.ts">Grafana
 *     ES API</a>
 */
@SuppressWarnings(
    "OptionalUsedAsFieldOrParameterType") // Per https://armeria.dev/docs/server-annotated-service/
public class ElasticsearchApiService {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchApiService.class);
  private final AstraQueryServiceBase searcher;

  private final OpenSearchRequest openSearchRequest = new OpenSearchRequest();
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final @NonNull Map<String, Object> FIELD_CAP_SEARCHABLE_NOT_AGGREGATABLE =
      getFieldCapMap(true, false);
  private static final @NonNull Map<String, Object> FIELD_CAP_NOT_SEARCHABLE_NOT_AGGREGATABLE =
      getFieldCapMap(false, false);
  private static final @NonNull Map<String, Object> FIELD_CAP_SEARCHABLE_AGGREGATABLE =
      getFieldCapMap(true, true);

  public ElasticsearchApiService(AstraQueryServiceBase searcher) {
    this.searcher = searcher;
  }

  /** Returns metadata about the cluster */
  @Get
  @Path("/")
  public HttpResponse clusterMetadata() {
    // todo - expand this to automatically pull in build info
    // example - https://opensearch.org/docs/2.3/quickstart/
    // number must validate with npm semver validate for grafana compatibility due to
    // https://github.com/grafana/grafana/blob/f74d5ff93ebe61e090994162be9b08bafcd5b7f0/public/app/plugins/datasource/elasticsearch/components/QueryEditor/MetricAggregationsEditor/MetricEditor.tsx#L54
    return HttpResponse.of(
        """
        {
            "version":
            {
                "distribution": "astra",
                "number": "0.0.1",
                "lucene_version": "9.7.0"
            }
        }
        """);
  }

  /**
   * Multisearch API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html">API
   *     doc</a>
   */
  @Post
  @Blocking
  @Path("/_msearch")
  public HttpResponse multiSearch(String postBody) throws Exception {
    LOG.debug("Search request: {}", postBody);

    CurrentTraceContext currentTraceContext = Tracing.current().currentTraceContext();
    try (var scope = new StructuredTaskScope<EsSearchResponse>()) {
      List<StructuredTaskScope.Subtask<EsSearchResponse>> requestSubtasks =
          openSearchRequest.parseHttpPostBody(postBody).stream()
              .map((request) -> scope.fork(currentTraceContext.wrap(() -> doSearch(request))))
              .toList();

      scope.join();
      SearchResponseMetadata responseMetadata =
          new SearchResponseMetadata(
              0,
              requestSubtasks.stream().map(StructuredTaskScope.Subtask::get).toList(),
              Map.of("traceId", getTraceId()));
      return HttpResponse.of(
          HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(responseMetadata));
    }
  }

  private EsSearchResponse doSearch(AstraSearch.SearchRequest searchRequest) {
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("ElasticsearchApiService.doSearch");
    AstraSearch.SearchResult searchResult = searcher.doSearch(searchRequest);

    span.tag("requestDataset", searchRequest.getDataset());
    span.tag("requestHowMany", String.valueOf(searchRequest.getHowMany()));
    span.tag("resultHitsCount", String.valueOf(searchResult.getHitsCount()));
    span.tag("resultTookMicros", String.valueOf(searchResult.getTookMicros()));
    span.tag("resultFailedNodes", String.valueOf(searchResult.getFailedNodes()));
    span.tag("resultTotalNodes", String.valueOf(searchResult.getTotalNodes()));
    span.tag("resultTotalSnapshots", String.valueOf(searchResult.getTotalNodes()));
    span.tag(
        "resultSnapshotsWithReplicas", String.valueOf(searchResult.getSnapshotsWithReplicas()));

    try {
      HitsMetadata hits = getHits(searchResult);
      return new EsSearchResponse.Builder()
          .hits(hits)
          .aggregations(parseAggregations(searchResult.getInternalAggregations()))
          .took(Duration.of(searchResult.getTookMicros(), ChronoUnit.MICROS).toMillis())
          .shardsMetadata(searchResult.getTotalNodes(), searchResult.getFailedNodes())
          .debugMetadata(Map.of())
          .status(200)
          .build();
    } catch (Exception e) {
      LOG.error("Error fulfilling request for multisearch query", e);
      span.error(e);
      return new EsSearchResponse.Builder()
          .took(Duration.of(searchResult.getTookMicros(), ChronoUnit.MICROS).toMillis())
          .shardsMetadata(searchResult.getTotalNodes(), searchResult.getFailedNodes())
          .status(500)
          .build();
    } finally {
      span.finish();
    }
  }

  private JsonNode parseAggregations(ByteString byteInput) throws IOException {
    InternalAggregation internalAggregations =
        OpenSearchInternalAggregation.fromByteArray(byteInput.toByteArray());
    if (internalAggregations != null) {
      return objectMapper.readTree(internalAggregations.toString());
    }
    return null;
  }

  private String getTraceId() {
    TraceContext traceContext = Tracing.current().currentTraceContext().get();
    if (traceContext != null) {
      return traceContext.traceIdString();
    }
    return "";
  }

  private HitsMetadata getHits(AstraSearch.SearchResult searchResult) throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<SearchResponseHit> responseHits = new ArrayList<>(hitsByteList.size());
    for (ByteString bytes : hitsByteList) {
      responseHits.add(SearchResponseHit.fromByteString(bytes));
    }

    return new HitsMetadata.Builder()
        .hitsTotal(ImmutableMap.of("value", responseHits.size(), "relation", "eq"))
        .hits(responseHits)
        .build();
  }

  /**
   * Mapping API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html">API
   *     doc</a>
   */
  @Get
  @Path("/:indexName/_mapping")
  public HttpResponse mapping(
      @Param("indexName") Optional<String> indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs)
      throws IOException {
    // Use a tree map so the results are naturally sorted
    Map<String, Map<String, String>> propertiesMap = new TreeMap<>();

    // we default the schema search to the last hour if params are not provided
    AstraSearch.SchemaResult schemaResult =
        searcher.getSchema(
            AstraSearch.SchemaRequest.newBuilder()
                .setDataset(indexName.orElse("*"))
                .setStartTimeEpochMs(
                    startTimeEpochMs.orElse(
                        Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()))
                .setEndTimeEpochMs(endTimeEpochMs.orElse(Instant.now().toEpochMilli()))
                .build());

    Map<String, FieldType> schema = SearchResultUtils.fromSchemaResultProto(schemaResult);
    schema.forEach((key, value) -> propertiesMap.put(key, Map.of("type", value.getName())));

    // todo - remove this after we add support for a "date" type
    // override the timestamp as a date field for proper autocomplete
    propertiesMap.put(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, Map.of("type", "date"));

    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        JsonUtil.writeAsString(
            ImmutableMap.of(
                indexName.orElseThrow(),
                ImmutableMap.of("mappings", ImmutableMap.of("properties", propertiesMap)))));
  }

  /**
   * Field Capabilities API
   *
   * @see <a
   *     href="https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-field-caps">API
   *     doc</a>
   */
  @Get
  @Path("/:indexName/_field_caps")
  public HttpResponse fieldCaps(
      @Param("indexName") Optional<String> indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("fields") Optional<List<String>> fields)
      throws IOException {
    // Use a tree map so the results are naturally sorted
    Map<String, Map<String, Map<String, Object>>> fieldsMap = new TreeMap<>();

    // we default the schema search to the last hour if params are not provided
    AstraSearch.SchemaResult schemaResult =
        searcher.getSchema(
            AstraSearch.SchemaRequest.newBuilder()
                .setDataset(indexName.orElse("*"))
                .setStartTimeEpochMs(
                    startTimeEpochMs.orElse(
                        Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()))
                .setEndTimeEpochMs(endTimeEpochMs.orElse(Instant.now().toEpochMilli()))
                .build());

    Map<String, FieldType> schema = SearchResultUtils.fromSchemaResultProto(schemaResult);

    if (fields.isPresent()) {
      List<String> fieldsOrEmpty = fields.orElse(List.of());
      List<String> withWildcard =
          fieldsOrEmpty.stream().filter(field -> field.contains("*")).toList();
      List<String> withoutWildcard =
          fieldsOrEmpty.stream().filter(field -> !field.contains("*")).toList();

      for (String field : withoutWildcard) {
        FieldType fieldType = schema.get(field);
        if (fieldType != null) {
          fieldsMap.put(
              field, Map.of(fieldType.getName(), getFlagsForType(indexName, field, fieldType)));
        }
      }
      withWildcard.stream()
          .map(field -> Pattern.quote(field).replace("*", "\\E.*\\Q"))
          .forEach(
              field -> {
                Pattern pattern = Pattern.compile("^" + field + "$");
                for (Map.Entry<String, FieldType> entry : schema.entrySet()) {
                  String fieldName = entry.getKey();
                  FieldType fieldType = entry.getValue();
                  if (pattern.matcher(fieldName).matches()) {
                    fieldsMap.put(
                        fieldName,
                        Map.of(
                            fieldType.getName(), getFlagsForType(indexName, fieldName, fieldType)));
                  }
                }
              });
    } else {
      schema.forEach(
          (key, value) ->
              fieldsMap.put(key, Map.of(value.getName(), getFlagsForType(indexName, key, value))));
    }
    // todo - remove this after we add support for a "date" type
    // override the timestamp as a date field for proper autocomplete
    if (fieldsMap.containsKey(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)) {
      fieldsMap.put(
          LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
          Map.of("date", FIELD_CAP_SEARCHABLE_AGGREGATABLE));
    }
    // indices: [...indices...]
    // fields:
    //   <name>:
    //     <type>:
    //       ...list of things about it
    //       metadata_field
    //       searchable
    //       aggregatable
    //       inference
    //       indices
    //       non_aggregatable_indices
    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        JsonUtil.writeAsString(ImmutableMap.of("fields", fieldsMap)));
  }

  private static Map<String, Object> getFlagsForType(
      Optional<String> indexName, String fieldName, FieldType fieldType) {
    return switch (fieldType.toSchemaFieldType()) {
      case KEYWORD,
          IP,
          DATE,
          DOUBLE,
          FLOAT,
          HALF_FLOAT,
          INTEGER,
          LONG,
          SCALED_LONG,
          SHORT,
          BYTE,
          ID,
          STRING,
          BOOLEAN ->
          FIELD_CAP_SEARCHABLE_AGGREGATABLE;
      case TEXT -> FIELD_CAP_SEARCHABLE_NOT_AGGREGATABLE;
      case BINARY -> FIELD_CAP_NOT_SEARCHABLE_NOT_AGGREGATABLE;
      default -> {
        LOG.warn("Unsupported field type {} for field {}", fieldType, fieldName);
        yield FIELD_CAP_NOT_SEARCHABLE_NOT_AGGREGATABLE;
      }
    };
  }

  private static @NonNull Map<String, Object> getFieldCapMap(
      Boolean searchable, Boolean aggregatable) {
    return Map.of(
        "metadata_field",
        Boolean.FALSE,
        "searchable",
        searchable,
        "aggregatable",
        aggregatable,
        "inference",
        Boolean.FALSE);
  }
}
