package com.slack.astra.logstore.duckdb;

import com.slack.astra.logstore.search.LogIndexSearcher;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.logstore.search.SourceFieldFilter;
import java.io.IOException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

// TODO: Implement searcher class.
public class DuckdbSearcher<T> implements LogIndexSearcher<T> {
  @Override
  public SearchResult<T> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
