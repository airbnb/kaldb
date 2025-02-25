package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadata;
import java.util.Objects;

public class PartitionMetadata extends AstraMetadata {
  public final String name;
  public final int partition_id;
  public final long utilization;

  public PartitionMetadata(int partition) {
    super(String.format("partition_%s", partition));
    this.name = String.format("partition_%s", partition);
    this.partition_id = partition;
    this.utilization = 0;
  }

  public PartitionMetadata getPartition() {
    return this;
  }

  public String getName() { return this.name; }

  public int getPartitionID() {
    return this.partition_id;
  }

  public long getUtilization() {
    return this.utilization;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PartitionMetadata)) return false;
    if (!super.equals(o)) return false;
    PartitionMetadata that = (PartitionMetadata) o;
    return utilization == that.utilization
        && name.equals(that.name)
        && partition_id == that.partition_id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name, partition_id, utilization);
  }

  @Override
  public String toString() {
    return "PartitionMetadata{"
        + "name='"
        + name
        + ", partition_id='"
        + partition_id
        + ", utilization="
        + utilization
        + '}';
  }
}
