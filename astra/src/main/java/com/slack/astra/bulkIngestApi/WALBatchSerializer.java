package com.slack.astra.bulkIngestApi;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.slack.astra.proto.wal.WalProtos;
import com.slack.service.murron.trace.Trace;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for serializing and compressing WAL batches for S3 storage.
 *
 * <p>Provides methods to serialize trace spans grouped by index into a compressed binary format and
 * deserialize them back. Uses GZIP compression and protobuf serialization with BatchHeader metadata
 * for each index section.
 *
 * <p>Thread-safe utility class with static methods only.
 */
public class WALBatchSerializer {

  public static byte[] serialize(Map<String, List<Trace.Span>> indexDocs) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {

      CodedOutputStream codedOut = CodedOutputStream.newInstance(gzipOut);

      for (Map.Entry<String, List<Trace.Span>> entry : indexDocs.entrySet()) {
        WalProtos.BatchHeader header =
            WalProtos.BatchHeader.newBuilder()
                .setIndex(entry.getKey())
                .setSpanCount(entry.getValue().size())
                .build();

        // Write header size then header using varint encoding
        codedOut.writeUInt32NoTag(header.getSerializedSize());
        header.writeTo(codedOut);

        // Write each span using varint encoding
        for (Trace.Span span : entry.getValue()) {
          codedOut.writeUInt32NoTag(span.getSerializedSize());
          span.writeTo(codedOut);
        }
      }

      codedOut.flush();
      gzipOut.finish();
      return baos.toByteArray();
    }
  }

  public static Map<String, List<Trace.Span>> deserialize(byte[] compressedData)
      throws IOException {

    Map<String, List<Trace.Span>> result = new HashMap<>();

    try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
        GZIPInputStream gzipIn = new GZIPInputStream(bais)) {

      CodedInputStream codedIn = CodedInputStream.newInstance(gzipIn);

      while (!codedIn.isAtEnd()) {
        try {
          // Read header size using varint encoding
          int headerSize = codedIn.readUInt32();

          // Use pushLimit/popLimit for safe bounded reading
          int previousLimit = codedIn.pushLimit(headerSize);
          WalProtos.BatchHeader header = WalProtos.BatchHeader.parseFrom(codedIn);
          codedIn.popLimit(previousLimit);

          String index = header.getIndex();
          int spanCount = header.getSpanCount();

          List<Trace.Span> spans = new ArrayList<>();
          for (int i = 0; i < spanCount; i++) {
            // Read span size using varint encoding
            int spanSize = codedIn.readUInt32();

            // Use pushLimit/popLimit for safe bounded reading
            int spanLimit = codedIn.pushLimit(spanSize);
            Trace.Span span = Trace.Span.parseFrom(codedIn);
            codedIn.popLimit(spanLimit);

            spans.add(span);
          }

          result.put(index, spans);

        } catch (IOException e) {
          if (codedIn.isAtEnd()) {
            break; // Natural end of stream
          }
          throw new IOException("Corruption detected during deserialization", e);
        }
      }
    }
    return result;
  }
}
