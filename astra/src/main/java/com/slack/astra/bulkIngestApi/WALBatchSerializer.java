package com.slack.astra.bulkIngestApi;

import com.slack.astra.proto.wal.WalProtos;
import com.slack.service.murron.trace.Trace;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    Map<String, List<Trace.Span>> result = new HashMap<>();

    try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
        GZIPInputStream gzipIn = new GZIPInputStream(bais)) {

      while (true) {
        try {
          // Read header size
          int headerSize = readInt(gzipIn);
          if (headerSize <= 0) {
            break; // No more headers to read
          }
          // Read header data
          byte[] headerData = gzipIn.readNBytes(headerSize);
          if (headerData.length != headerSize) {
            throw new IOException("Failed to read complete header data");
          }

          WalProtos.BatchHeader header = WalProtos.BatchHeader.parseFrom(headerData);
          String index = header.getIndex();
          int spanCount = header.getSpanCount();

          List<Trace.Span> spans = new ArrayList<>();
          for (int i = 0; i < spanCount; i++) {
            // Read span size (4 bytes)
            int spanSize = readInt(gzipIn);
            if (spanSize <= 0) break;

            // Read span data
            byte[] spanBytes = gzipIn.readNBytes(spanSize);
            if (spanBytes.length != spanSize) break;

            Trace.Span span = Trace.Span.parseFrom(spanBytes);
            spans.add(span);
          }

          result.put(index, spans);

        } catch (EOFException e) {
          break; // Natural end of stream
        } catch (IOException e) {
          throw new IOException("Corruption detected during deserialization", e);
        }
      }
    }
    return result;
  }

  private static void writeInt(OutputStream out, int value) throws IOException {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  private static int readInt(InputStream in) throws IOException {
    int b1 = in.read();
    int b2 = in.read();
    int b3 = in.read();
    int b4 = in.read();

    if (b1 < 0 || b2 < 0 || b3 < 0 || b4 < 0) {
      throw new IOException("Unexpected end of stream");
    }

    return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
  }
}
