package com.slack.astra.bulkIngestApi;

import com.slack.astra.proto.wal.WalProtos;
import com.slack.service.murron.trace.Trace;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class WALBatchSerializer {

  public static byte[] serializeAndCompress(Map<String, List<Trace.Span>> indexDocs)
      throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {

      for (Map.Entry<String, List<Trace.Span>> entry : indexDocs.entrySet()) {
        WalProtos.BatchHeader header =
            WalProtos.BatchHeader.newBuilder()
                .setIndex(entry.getKey())
                .setSpanCount(entry.getValue().size())
                .build();

        // Write header size then header - using protobuf methods
        writeInt(gzipOut, header.getSerializedSize());
        header.writeTo(gzipOut);

        // Write each span - using protobuf methods
        for (Trace.Span span : entry.getValue()) {
          writeInt(gzipOut, span.getSerializedSize());
          span.writeTo(gzipOut);
        }
      }
      gzipOut.finish();
      return baos.toByteArray();
    }
  }

  public static Map<String, List<Trace.Span>> deserializeAndDecompress(byte[] compressedData)
      throws IOException {
    // TODO: Implement for indexers
    return new HashMap<>();
  }

  private static void writeInt(OutputStream out, int value) throws IOException {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }
}
