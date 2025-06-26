package com.slack.astra.metadata.partition;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.manager_api.ManagerApi;
import com.slack.astra.proto.metadata.Metadata;
import org.junit.jupiter.api.Test;

public class PartitionMetadataSerializerTest {
  private final PartitionMetadataSerializer serDe = new PartitionMetadataSerializer();

  @Test
  public void testPartitionMetadataSerializer() throws InvalidProtocolBufferException {
    final String partitionId = "1";
    final long maxCapacity = 5000000;

    final PartitionMetadata partitionMetadata = new PartitionMetadata(partitionId, maxCapacity);

    String serializedDatasetMetadata = serDe.toJsonStr(partitionMetadata);
    assertThat(serializedDatasetMetadata).isNotEmpty();

    PartitionMetadata deserializedPartitionMetadata = serDe.fromJsonStr(serializedDatasetMetadata);
    assertThat(deserializedPartitionMetadata).isEqualTo(partitionMetadata);

    assertThat(deserializedPartitionMetadata.name).isEqualTo(partitionId);
    assertThat(deserializedPartitionMetadata.partitionId).isEqualTo(partitionId);
    assertThat(deserializedPartitionMetadata.maxCapacity).isEqualTo(maxCapacity);
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
    final long maxCapacity = 5000000;

    final PartitionMetadata partitionMetadata = new PartitionMetadata(partitionId, maxCapacity);

    Metadata.PartitionMetadata partitionMetadataProto =
        PartitionMetadataSerializer.toPartitionMetadataProto(partitionMetadata);

    assertThat(partitionMetadataProto.getPartitionId()).isEqualTo(partitionId);
    assertThat(partitionMetadataProto.getMaxCapacity()).isEqualTo(maxCapacity);

    PartitionMetadata partitionMetadataFromProto =
        PartitionMetadataSerializer.fromPartitionMetadataProto(partitionMetadataProto);

    assertThat(partitionMetadataFromProto.name).isEqualTo(partitionId);
    assertThat(partitionMetadataFromProto.partitionId).isEqualTo(partitionId);
    assertThat(partitionMetadataFromProto.maxCapacity).isEqualTo(maxCapacity);
  }

  @Test
  public void testCalculatedPartitionMetadata() {
    final String partitionId = "1";
    final long provisionedCapacity = 1000000;
    final long maxCapacity = 5000000;
    final boolean dedicatedPartition = false;

    final CalculatedPartitionMetadata partitionMetadata =
        new CalculatedPartitionMetadata(
            partitionId, provisionedCapacity, maxCapacity, dedicatedPartition);

    ManagerApi.CalculatedPartitionMetadata partitionMetadataProto =
        PartitionMetadataSerializer.toCalculatedPartitionMetadataProto(partitionMetadata);

    assertThat(partitionMetadataProto.getPartitionId()).isEqualTo(partitionId);
    assertThat(partitionMetadataProto.getProvisionedCapacity()).isEqualTo(provisionedCapacity);
    assertThat(partitionMetadataProto.getMaxCapacity()).isEqualTo(maxCapacity);
    assertThat(partitionMetadataProto.getDedicatedPartition()).isEqualTo(dedicatedPartition);

    CalculatedPartitionMetadata partitionMetadataFromProto =
        PartitionMetadataSerializer.fromCalculatedPartitionMetadataProto(partitionMetadataProto);

    assertThat(partitionMetadataFromProto.partitionId).isEqualTo(partitionId);
    assertThat(partitionMetadataFromProto.provisionedCapacity).isEqualTo(provisionedCapacity);
    assertThat(partitionMetadataFromProto.maxCapacity).isEqualTo(maxCapacity);
    assertThat(partitionMetadataFromProto.dedicatedPartition).isEqualTo(dedicatedPartition);
  }
}
