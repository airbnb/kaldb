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
    final long provisionedCapacity = 100000;
    final long maxCapacity = 5000000;
    final boolean dedicatedPartition = false;

    final PartitionMetadata partitionMetadata =
        new PartitionMetadata(partitionId, provisionedCapacity, maxCapacity, dedicatedPartition);

    String serializedDatasetMetadata = serDe.toJsonStr(partitionMetadata);
    assertThat(serializedDatasetMetadata).isNotEmpty();

    PartitionMetadata deserializedPartitionMetadata = serDe.fromJsonStr(serializedDatasetMetadata);
    assertThat(deserializedPartitionMetadata).isEqualTo(partitionMetadata);

    assertThat(deserializedPartitionMetadata.name).isEqualTo(partitionId);
    assertThat(deserializedPartitionMetadata.partitionId).isEqualTo(partitionId);
    assertThat(deserializedPartitionMetadata.provisionedCapacity).isEqualTo(provisionedCapacity);
    assertThat(deserializedPartitionMetadata.maxCapacity).isEqualTo(maxCapacity);
    assertThat(deserializedPartitionMetadata.dedicatedPartition).isEqualTo(false);
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
    final long provisionedCapacity = 1000000;
    final long maxCapacity = 5000000;
    final boolean dedicatedPartition = false;

    final PartitionMetadata partitionMetadata =
        new PartitionMetadata(partitionId, provisionedCapacity, maxCapacity, dedicatedPartition);

    Metadata.PartitionMetadata partitionMetadataProto =
        PartitionMetadataSerializer.toPartitionMetadataProto(partitionMetadata);

    assertThat(partitionMetadataProto.getPartitionId()).isEqualTo(partitionId);
    assertThat(partitionMetadataProto.getProvisionedCapacity()).isEqualTo(provisionedCapacity);
    assertThat(partitionMetadataProto.getMaxCapacity()).isEqualTo(maxCapacity);
    assertThat(partitionMetadataProto.getDedicatedPartition()).isEqualTo(dedicatedPartition);

    PartitionMetadata partitionMetadataFromProto =
        PartitionMetadataSerializer.fromPartitionMetadataProto(partitionMetadataProto);

    assertThat(partitionMetadataFromProto.name).isEqualTo(partitionId);
    assertThat(partitionMetadataFromProto.partitionId).isEqualTo(partitionId);
    assertThat(partitionMetadataFromProto.provisionedCapacity).isEqualTo(provisionedCapacity);
    assertThat(partitionMetadataFromProto.maxCapacity).isEqualTo(maxCapacity);
    assertThat(partitionMetadataFromProto.dedicatedPartition).isEqualTo(dedicatedPartition);
  }
}
