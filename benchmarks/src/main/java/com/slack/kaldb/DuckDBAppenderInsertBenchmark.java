package com.slack.kaldb;

import org.openjdk.jmh.annotations.*;
import org.duckdb.*;

import java.sql.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(1)
public class DuckDBAppenderInsertBenchmark {

    private Connection conn;
    private DuckDBAppender appender;
    private PreparedStatement pstmt;

    @Param({"base", "extra10", "extra100"})
    private String tableVariant;

    private final int numRows = 10_000;
    private int extraColumns;

    @Setup(Level.Invocation)
    public void setup() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        conn.setAutoCommit(false);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS items");
            stmt.execute(createTableDDL(tableVariant));
        }

        extraColumns = tableVariant.equals("extra10") ? 10 :
                tableVariant.equals("extra100") ? 100 : 0;

        appender = new DuckDBAppender((DuckDBConnection) conn, "", "items");
        pstmt = conn.prepareStatement(buildInsertSQL(extraColumns));
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws Exception {
        appender.close();
        pstmt.close();
        conn.close();
    }

    @Benchmark
    public void insertRowsWithAppender() throws Exception {
        for (int i = 0; i < numRows; i++) {
            appender.beginRow();
            appender.append("id_" + i);
            appender.append("parent_" + i);
            appender.append("trace_" + i);
            appender.append("name_" + i);
            appender.append(String.valueOf(new Timestamp(System.currentTimeMillis())));
            appender.append((float) i);
            for (int j = 0; j < extraColumns; j++) {
                appender.append("extra_" + j + "_" + i);
            }
            appender.endRow();
        }
        appender.flush();
        conn.commit();
    }

    @Benchmark
    public void insertRowsWithPreparedStatement() throws Exception {
        conn.setAutoCommit(false); // begin transaction

        for (int i = 0; i < numRows; i++) {
            int idx = 1;
            pstmt.setString(idx++, "id_" + i);
            pstmt.setString(idx++, "parent_" + i);
            pstmt.setString(idx++, "trace_" + i);
            pstmt.setString(idx++, "name_" + i);
            pstmt.setTimestamp(idx++, new Timestamp(System.currentTimeMillis()));
            pstmt.setFloat(idx++, (float) i);
            for (int j = 0; j < extraColumns; j++) {
                pstmt.setString(idx++, "extra_" + j + "_" + i);
            }
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true); // end transaction
    }

    private String createTableDDL(String variant) {
        StringBuilder ddl = new StringBuilder(
                "CREATE OR REPLACE TABLE items (" +
                        "id STRING," +
                        "parent_id STRING," +
                        "trace_id STRING," +
                        "name STRING," +
                        "timestamp TIMESTAMP," +
                        "duration FLOAT"
        );

        int extras = variant.equals("extra10") ? 10 :
                variant.equals("extra100") ? 100 : 0;
        for (int i = 0; i < extras; i++) {
            ddl.append(", extra_col_").append(i).append(" STRING");
        }

        ddl.append(");");
        return ddl.toString();
    }

    private String buildInsertSQL(int extras) {
        StringBuilder sql = new StringBuilder("INSERT INTO items (id, parent_id, trace_id, name, timestamp, duration");
        for (int i = 0; i < extras; i++) {
            sql.append(", extra_col_").append(i);
        }
        sql.append(") VALUES (");
        int totalCols = 6 + extras;
        sql.append("?,".repeat(totalCols - 1)).append("?");
        sql.append(")");
        return sql.toString();
    }
}
