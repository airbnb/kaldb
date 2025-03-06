package com.slack.astra.metadata.partition;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.metadata.Metadata;
import org.junit.jupiter.api.Test;

public class PartitionMetadataSerializerTest {
  private final PartitionMetadataSerializer serDe = new PartitionMetadataSerializer();

  @Test
  public void testPartitionMetadataSerializer() throws InvalidProtocolBufferException {
    final String partitionId = "1";
    final long utilization = 100000;
    final boolean isPartitionShared = false;

    final PartitionMetadata partitionMetadata =
        new PartitionMetadata(partitionId, utilization, isPartitionShared);

    String serializedDatasetMetadata = serDe.toJsonStr(partitionMetadata);
    assertThat(serializedDatasetMetadata).isNotEmpty();

    PartitionMetadata deserializedPartitionMetadata = serDe.fromJsonStr(serializedDatasetMetadata);
    assertThat(deserializedPartitionMetadata).isEqualTo(partitionMetadata);

    assertThat(deserializedPartitionMetadata.name).isEqualTo(partitionId);
    assertThat(deserializedPartitionMetadata.partitionId).isEqualTo(partitionId);
    assertThat(deserializedPartitionMetadata.utilization).isEqualTo(utilization);
    assertThat(deserializedPartitionMetadata.isPartitionShared).isEqualTo(false);
  }

  @Test
  public void testInvalidSerializations() {
    Throwable serializeNull = catchThrowable(() -> serDe.toJsonStr(null));
    assertThat(serializeNull).isInstanceOf(IllegalArgumentException.class);

    Throwable deserializeNull = catchThrowable(() -> serDe.fromJsonStr(null));
    assertThat(deserializeNull).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeEmpty = catchThrowable(() -> serDe.fromJsonStr(""));
    assertThat(deserializeEmpty).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeCorrupt = catchThrowable(() -> serDe.fromJsonStr("test"));
    assertThat(deserializeCorrupt).isInstanceOf(InvalidProtocolBufferException.class);
  }

  @Test
  public void testPartitionMetadata() {
    final String partitionId = "1";
    final long utilization = 1000000;
    final boolean isPartitionShared = false;

    final PartitionMetadata partitionMetadata =
        new PartitionMetadata(partitionId, utilization, isPartitionShared);

    Metadata.PartitionMetadata partitionMetadataProto =
        PartitionMetadataSerializer.toPartitionMetadataProto(partitionMetadata);

    assertThat(partitionMetadataProto.getPartitionId()).isEqualTo(partitionId);
    assertThat(partitionMetadataProto.getUtilization()).isEqualTo(utilization);
    assertThat(partitionMetadataProto.getIsPartitionShared()).isEqualTo(isPartitionShared);

    PartitionMetadata partitionMetadataFromProto =
        PartitionMetadataSerializer.fromPartitionMetadataProto(partitionMetadataProto);

    assertThat(partitionMetadataFromProto.name).isEqualTo(partitionId);
    assertThat(partitionMetadataFromProto.partitionId).isEqualTo(partitionId);
    assertThat(partitionMetadataFromProto.utilization).isEqualTo(utilization);
    assertThat(partitionMetadataFromProto.isPartitionShared).isEqualTo(isPartitionShared);
  }
}
