package com.slack.astra.server;

import static com.slack.astra.metadata.dataset.DatasetMetadataSerializer.toDatasetMetadataProto;
import static com.slack.astra.metadata.partition.PartitionMetadataSerializer.toPartitionMetadataProto;
import static org.awaitility.Awaitility.await;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.slack.astra.chunk.ChunkInfo;
import com.slack.astra.clusterManager.ReplicaRestoreService;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataSerializer;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadataSerializer;
import com.slack.astra.metadata.partition.PartitionMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.manager_api.ManagerApi;
import com.slack.astra.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.astra.proto.metadata.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.naming.SizeLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Administration API for managing dataset configurations, including throughput and partition
 * assignments. This API is available only on the cluster manager service, and the data created is
 * consumed primarily by the pre-processor and query services.
 */
public class ManagerApiGrpc extends ManagerApiServiceGrpc.ManagerApiServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagerApiGrpc.class);
  private final DatasetMetadataStore datasetMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final PartitionMetadataStore partitionMetadataStore;
  public static final long MAX_TIME = Long.MAX_VALUE;
  // TODO: add to config
  public static final long PARTITION_START_TIME_OFFSET = 15 * 60 * 1000; // 15 minutes
  private final ReplicaRestoreService replicaRestoreService;

  public final long maxPartitionCapacity;
  public final int minNumberOfPartitions;

  public ManagerApiGrpc(
      DatasetMetadataStore datasetMetadataStore,
      PartitionMetadataStore partitionMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      ReplicaRestoreService replicaRestoreService,
      long maxPartitionCapacity,
      int minNumberOfPartitions) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.replicaRestoreService = replicaRestoreService;
    this.partitionMetadataStore = partitionMetadataStore;
    this.maxPartitionCapacity = maxPartitionCapacity;
    this.minNumberOfPartitions = minNumberOfPartitions;
  }

  /** Initializes a new dataset in the metadata store with no initial allocated capacity */
  @Override
  public void createDatasetMetadata(
      ManagerApi.CreateDatasetMetadataRequest request,
      StreamObserver<Metadata.DatasetMetadata> responseObserver) {

    try {
      datasetMetadataStore.createSync(
          new DatasetMetadata(
              request.getName(),
              request.getOwner(),
              0L,
              Collections.emptyList(),
              request.getServiceNamePattern()));
      responseObserver.onNext(
          toDatasetMetadataProto(datasetMetadataStore.getSync(request.getName())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error creating new dataset", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Updates an existing dataset with new metadata */
  @Override
  public void updateDatasetMetadata(
      ManagerApi.UpdateDatasetMetadataRequest request,
      StreamObserver<Metadata.DatasetMetadata> responseObserver) {

    try {
      DatasetMetadata existingDatasetMetadata = datasetMetadataStore.getSync(request.getName());

      DatasetMetadata updatedDatasetMetadata =
          new DatasetMetadata(
              existingDatasetMetadata.getName(),
              request.getOwner(),
              existingDatasetMetadata.getThroughputBytes(),
              existingDatasetMetadata.getPartitionConfigs(),
              request.getServiceNamePattern());
      datasetMetadataStore.updateSync(updatedDatasetMetadata);
      responseObserver.onNext(toDatasetMetadataProto(updatedDatasetMetadata));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error updating existing dataset", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Returns a single dataset metadata by name */
  @Override
  public void getDatasetMetadata(
      ManagerApi.GetDatasetMetadataRequest request,
      StreamObserver<Metadata.DatasetMetadata> responseObserver) {

    try {
      responseObserver.onNext(
          toDatasetMetadataProto(datasetMetadataStore.getSync(request.getName())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting dataset", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Returns all available datasets from the metadata store */
  @Override
  public void listDatasetMetadata(
      ManagerApi.ListDatasetMetadataRequest request,
      StreamObserver<ManagerApi.ListDatasetMetadataResponse> responseObserver) {
    // todo - consider adding search/pagination support
    try {
      responseObserver.onNext(
          ManagerApi.ListDatasetMetadataResponse.newBuilder()
              .addAllDatasetMetadata(
                  datasetMetadataStore.listSync().stream()
                      .map(DatasetMetadataSerializer::toDatasetMetadataProto)
                      .collect(Collectors.toList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting datasets.", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Delete a data from the metadata store */
  @Override
  public void deleteDatasetMetadata(
      ManagerApi.DeleteDatasetMetadataRequest request,
      StreamObserver<ManagerApi.DeleteDatasetMetadataResponse> responseObserver) {

    try {
      DatasetMetadata existingDatasetMetadata = datasetMetadataStore.getSync(request.getName());
      Optional<DatasetPartitionMetadata> previousActiveDatasetPartition =
          existingDatasetMetadata.getPartitionConfigs().stream()
              .filter(
                  datasetPartitionMetadata ->
                      datasetPartitionMetadata.getEndTimeEpochMs() == MAX_TIME)
              .findFirst();

      if (previousActiveDatasetPartition.isPresent()) {
        Map<String, PartitionMetadata> _ =
            clearPartitions(
                previousActiveDatasetPartition.get().getPartitions(),
                Math.ceilDiv(
                    existingDatasetMetadata.getThroughputBytes(),
                    previousActiveDatasetPartition.get().getPartitions().size()));
      }
      datasetMetadataStore.deleteSync(request.getName());
      responseObserver.onNext(
          ManagerApi.DeleteDatasetMetadataResponse.newBuilder()
              .setStatus(
                  String.format("Deleted dataset metadata %s successfully.", request.getName()))
              .build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      LOG.error("Error deleting dataset metadata ", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /**
   * Allocates a new partition assignment for a dataset. If a rate and a list of partition IDs are
   * provided, it will use it use the list of partition ids as the current allocation and
   * invalidates the existing assignment.
   */
  @Override
  public void updatePartitionAssignment(
      ManagerApi.UpdatePartitionAssignmentRequest request,
      StreamObserver<ManagerApi.UpdatePartitionAssignmentResponse> responseObserver) {

    try {
      // todo - add additional validation to ensure the provided allocation makes sense for the
      //  configured throughput values.
      Preconditions.checkArgument(
          request.getPartitionIdsList().stream().noneMatch(String::isBlank),
          "PartitionIds list must not contain blank strings");
      DatasetMetadata datasetMetadata = datasetMetadataStore.getSync(request.getName());
      // if the user provided a non-negative value for throughput set it, otherwise default to the
      // existing value
      long updatedThroughputBytes =
          request.getThroughputBytes() < 0
              ? datasetMetadata.getThroughputBytes()
              : request.getThroughputBytes();

      List<String> partitionIdList = List.of();

      if (request.getPartitionIdsList().isEmpty()) {
        partitionIdList =
            autoAssignPartition(
                datasetMetadata, updatedThroughputBytes, request.getRequireDedicatedPartition());
      } else {
        partitionIdList = request.getPartitionIdsList();
        manualAssignPartition(
            datasetMetadata,
            partitionIdList,
            updatedThroughputBytes,
            request.getRequireDedicatedPartition());
      }
      if (partitionIdList.isEmpty()) {
        String msg = "Error updating partition assignment, could not find partitions to assign";
        LOG.error(msg);
        responseObserver.onError(Status.UNKNOWN.withDescription(msg).asException());
        return;
      }
      ImmutableList<DatasetPartitionMetadata> updatedDatasetPartitionMetadata =
          addNewPartition(datasetMetadata.getPartitionConfigs(), partitionIdList);

      DatasetMetadata updatedDatasetMetadata =
          new DatasetMetadata(
              datasetMetadata.getName(),
              datasetMetadata.getOwner(),
              updatedThroughputBytes,
              updatedDatasetPartitionMetadata,
              datasetMetadata.getServiceNamePattern());
      datasetMetadataStore.updateSync(updatedDatasetMetadata);

      responseObserver.onNext(
          ManagerApi.UpdatePartitionAssignmentResponse.newBuilder()
              .addAllAssignedPartitionIds(partitionIdList)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error updating partition assignment", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void restoreReplica(
      ManagerApi.RestoreReplicaRequest request,
      StreamObserver<ManagerApi.RestoreReplicaResponse> responseObserver) {
    try {
      Preconditions.checkArgument(
          request.getStartTimeEpochMs() < request.getEndTimeEpochMs(),
          "Start time must not be after end time");
      Preconditions.checkArgument(
          !request.getServiceName().isEmpty(), "Service name must not be empty");

      List<SnapshotMetadata> snapshotsToRestore =
          calculateRequiredSnapshots(
              snapshotMetadataStore.listSync(),
              datasetMetadataStore,
              request.getStartTimeEpochMs(),
              request.getEndTimeEpochMs(),
              request.getServiceName());

      replicaRestoreService.queueSnapshotsForRestoration(snapshotsToRestore);

      responseObserver.onNext(
          ManagerApi.RestoreReplicaResponse.newBuilder().setStatus("success").build());
      responseObserver.onCompleted();
    } catch (SizeLimitExceededException e) {
      LOG.error(
          "Error handling request: number of replicas requested exceeds maxReplicasPerRequest limit",
          e);
      responseObserver.onError(
          Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage()).asException());
    } catch (Exception e) {
      LOG.error("Error handling request", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void restoreReplicaIds(
      ManagerApi.RestoreReplicaIdsRequest request,
      StreamObserver<ManagerApi.RestoreReplicaIdsResponse> responseObserver) {
    try {
      List<SnapshotMetadata> snapshotsToRestore =
          calculateRequiredSnapshots(
              request.getIdsToRestoreList(), snapshotMetadataStore.listSync());

      replicaRestoreService.queueSnapshotsForRestoration(snapshotsToRestore);

      responseObserver.onNext(
          ManagerApi.RestoreReplicaIdsResponse.newBuilder().setStatus("success").build());
      responseObserver.onCompleted();
    } catch (SizeLimitExceededException e) {
      LOG.error(
          "Error handling request: number of replicas requested exceeds maxReplicasPerRequest limit",
          e);
      responseObserver.onError(
          Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage()).asException());
    } catch (Exception e) {
      LOG.error("Error handling request", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /**
   * Determines all SnapshotMetadata between startTimeEpochMs and endTimeEpochMs that contain data
   * from the queried service
   *
   * @return List of SnapshotMetadata that are within specified timeframe and from queried service
   */
  protected static List<SnapshotMetadata> calculateRequiredSnapshots(
      List<SnapshotMetadata> snapshotMetadataList,
      DatasetMetadataStore datasetMetadataStore,
      long startTimeEpochMs,
      long endTimeEpochMs,
      String datasetName) {
    Set<String> partitionIdsWithQueriedData = new HashSet<>();
    List<DatasetPartitionMetadata> partitionMetadataList =
        DatasetPartitionMetadata.findPartitionsToQuery(
            datasetMetadataStore, startTimeEpochMs, endTimeEpochMs, datasetName);

    // flatten all partition ids into one list
    for (DatasetPartitionMetadata datasetPartitionMetadata : partitionMetadataList) {
      partitionIdsWithQueriedData.addAll(datasetPartitionMetadata.partitions);
    }

    List<SnapshotMetadata> snapshotMetadata = new ArrayList<>();

    for (SnapshotMetadata snapshot : snapshotMetadataList) {
      if (snapshotContainsRequestedDataAndIsWithinTimeframe(
          startTimeEpochMs, endTimeEpochMs, partitionIdsWithQueriedData, snapshot)) {
        snapshotMetadata.add(snapshot);
      }
    }

    return snapshotMetadata;
  }

  /**
   * Determines all SnapshotMetadata that match the IDs in snapshotIds
   *
   * @return List of SnapshotMetadata that are within specified timeframe and from queried service
   */
  protected static List<SnapshotMetadata> calculateRequiredSnapshots(
      List<String> snapshotIds, List<SnapshotMetadata> snapshotMetadataList) {
    Set<String> matchingSnapshots =
        Sets.intersection(
            Sets.newHashSet(snapshotIds),
            Sets.newHashSet(
                snapshotMetadataList.stream()
                    .map((snapshot) -> snapshot.snapshotId)
                    .collect(Collectors.toList())));

    return snapshotMetadataList.stream()
        .filter((snapshot) -> matchingSnapshots.contains(snapshot.snapshotId))
        .collect(Collectors.toList());
  }

  /**
   * Returns true if the given Snapshot: 1. contains data between startTimeEpochMs and
   * endTimeEpochMs; AND 2. is from one of the partitions containing data from the queried service
   */
  private static boolean snapshotContainsRequestedDataAndIsWithinTimeframe(
      long startTimeEpochMs,
      long endTimeEpochMs,
      Set<String> partitionIdsWithQueriedData,
      SnapshotMetadata snapshot) {
    return ChunkInfo.containsDataInTimeRange(
            snapshot.startTimeEpochMs, snapshot.endTimeEpochMs, startTimeEpochMs, endTimeEpochMs)
        && partitionIdsWithQueriedData.contains(snapshot.partitionId);
  }

  /**
   * Returns a new list of dataset partition metadata, with the provided partition IDs as the
   * current active assignment. This finds the current active assignment (end time of max long),
   * sets it to the current time, and then appends a new dataset partition assignment starting from
   * current time + 1 to max long.
   */
  private static ImmutableList<DatasetPartitionMetadata> addNewPartition(
      List<DatasetPartitionMetadata> existingPartitions, List<String> newPartitionIdsList) {
    if (newPartitionIdsList.isEmpty()) {
      return ImmutableList.copyOf(existingPartitions);
    }

    Optional<DatasetPartitionMetadata> previousActiveDatasetPartition =
        existingPartitions.stream()
            .filter(
                datasetPartitionMetadata ->
                    datasetPartitionMetadata.getEndTimeEpochMs() == MAX_TIME)
            .findFirst();

    List<DatasetPartitionMetadata> remainingDatasetPartitions =
        existingPartitions.stream()
            .filter(
                datasetPartitionMetadata ->
                    datasetPartitionMetadata.getEndTimeEpochMs() != MAX_TIME)
            .collect(Collectors.toList());

    // todo - consider adding some padding to this value; this may complicate
    //   validation as you would need to consider what happens when there's a future
    //   cut-over already scheduled
    // todo - if introducing an optional padding this should be added as a method parameter
    //   see https://github.com/slackhq/astra/pull/244#discussion_r835424863
    long partitionCutoverTime = Instant.now().toEpochMilli() + PARTITION_START_TIME_OFFSET;

    ImmutableList.Builder<DatasetPartitionMetadata> builder =
        ImmutableList.<DatasetPartitionMetadata>builder().addAll(remainingDatasetPartitions);

    if (previousActiveDatasetPartition.isPresent()) {
      DatasetPartitionMetadata updatedPreviousActivePartition =
          new DatasetPartitionMetadata(
              previousActiveDatasetPartition.get().getStartTimeEpochMs(),
              partitionCutoverTime,
              previousActiveDatasetPartition.get().getPartitions());
      builder.add(updatedPreviousActivePartition);
    }

    DatasetPartitionMetadata newPartitionMetadata =
        new DatasetPartitionMetadata(partitionCutoverTime + 1, MAX_TIME, newPartitionIdsList);
    return builder.add(newPartitionMetadata).build();
  }

  @Override
  public void resetPartitionData(
      ManagerApi.ResetPartitionDataRequest request,
      StreamObserver<ManagerApi.ResetPartitionDataResponse> responseObserver) {
    List<SnapshotMetadata> snapshotMetadataList = snapshotMetadataStore.listSync();

    int resetCount = 0;
    for (SnapshotMetadata snapshotMetadata : snapshotMetadataList) {
      if (Objects.equals(snapshotMetadata.partitionId, request.getPartitionId())) {
        if (!request.getDryRun()) {
          snapshotMetadata.maxOffset = 0;
          snapshotMetadataStore.updateSync(snapshotMetadata);
        }
        resetCount++;
      }
    }

    if (request.getDryRun()) {
      responseObserver.onNext(
          ManagerApi.ResetPartitionDataResponse.newBuilder()
              .setStatus(
                  String.format(
                      "%s snapshots matching partitionId '%s' out of %s total snapshots, none were reset as this was a dry-run.",
                      resetCount, request.getPartitionId(), snapshotMetadataList.size()))
              .build());
    } else {
      responseObserver.onNext(
          ManagerApi.ResetPartitionDataResponse.newBuilder()
              .setStatus(
                  String.format(
                      "Reset %s snapshots matching partitionId '%s' out of %s total snapshots.",
                      resetCount, request.getPartitionId(), snapshotMetadataList.size()))
              .build());
    }

    responseObserver.onCompleted();
  }

  @Override
  public void createPartition(
      ManagerApi.CreatePartitionRequest request,
      StreamObserver<Metadata.PartitionMetadata> responseObserver) {
    try {
      long maxCapacity =
          request.getMaxCapacity() == 0 ? maxPartitionCapacity : request.getMaxCapacity();

      try {
        PartitionMetadata partitionMetadata =
            partitionMetadataStore.getSync(request.getPartitionId());
        // update
        long provisionedCapacity =
            request.getProvisionedCapacity() < 0
                ? request.getProvisionedCapacity()
                : partitionMetadata.provisionedCapacity;
        PartitionMetadata updatedPartitionMetadata =
            new PartitionMetadata(
                partitionMetadata.partitionId,
                provisionedCapacity,
                maxCapacity,
                request.getDedicatedPartition());
        partitionMetadataStore.updateSync(updatedPartitionMetadata);
        responseObserver.onNext(toPartitionMetadataProto(updatedPartitionMetadata));
        responseObserver.onCompleted();
      } catch (Exception InternalMetadataStoreException) {
        // create
        PartitionMetadata createPartitionMetadata =
            new PartitionMetadata(
                request.getPartitionId(),
                request.getProvisionedCapacity(),
                maxCapacity,
                request.getDedicatedPartition());
        partitionMetadataStore.createSync(createPartitionMetadata);
        responseObserver.onNext(toPartitionMetadataProto(createPartitionMetadata));
        responseObserver.onCompleted();
      }

    } catch (Exception e) {
      LOG.error("Error creating new partition", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void listPartition(
      ManagerApi.ListPartitionRequest request,
      StreamObserver<Metadata.ListPartitionMetadataResponse> responseObserver) {
    try {
      responseObserver.onNext(
          Metadata.ListPartitionMetadataResponse.newBuilder()
              .addAllPartitionMetadata(
                  partitionMetadataStore.listSync().stream()
                      .map(PartitionMetadataSerializer::toPartitionMetadataProto)
                      .collect(Collectors.toList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error fetching partition list", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /**
   * Returns the partition count based of the throughput. Performs div operation, and takes the ceil
   * of the division. Also, checks if minimum partition requirement is met
   *
   * @param throughput service throughput to calculate number of partitions for
   * @return integer number of partition count
   */
  public int getPartitionCount(long throughput) {
    int numberOfPartitions = (int) Math.ceilDiv(throughput, maxPartitionCapacity);
    return numberOfPartitions < minNumberOfPartitions
        ? numberOfPartitions + (minNumberOfPartitions - numberOfPartitions)
        : numberOfPartitions;
  }

  /**
   * finds the available partitions based provisionedCapacity and updates the provisionedCapacity if
   * partitions are considered.
   *
   * @param requiredThroughput - targeted throughput
   * @param requireDedicatedPartition - if dedicated partitions are required
   * @param existingPartitionIds - list of existing partitions already to be re-used
   * @return list of string of partition ids
   */
  public List<String> findPartition(
      long requiredThroughput,
      boolean requireDedicatedPartition,
      List<String> existingPartitionIds) {
    List<String> partitionIdsList = new ArrayList<>();
    int numberOfPartitions = getPartitionCount(requiredThroughput);

    long perPartitionCapacity = Math.ceilDiv(requiredThroughput, numberOfPartitions);
    numberOfPartitions = numberOfPartitions - existingPartitionIds.size();

    List<PartitionMetadata> partitionMetadataList = partitionMetadataStore.listSync();
    partitionMetadataList.sort(
        Comparator.comparing(partitionMetadata -> partitionMetadata.partitionId));

    for (PartitionMetadata partitionMetadata : partitionMetadataList) {
      if (existingPartitionIds.contains(partitionMetadata.getPartitionID())) {
        continue;
      }
      if (requireDedicatedPartition) {
        if (partitionMetadata.getProvisionedCapacity() == 0) {
          partitionIdsList.add(partitionMetadata.getPartitionID());
        }
      } else {
        // partition has capacity and (partition is shared or (partition not shared and
        // provisionedCapacity = 0))
        if (partitionMetadata.provisionedCapacity + perPartitionCapacity <= maxPartitionCapacity
            && (!partitionMetadata.dedicatedPartition
                || partitionMetadata.getProvisionedCapacity() == 0)) {
          partitionIdsList.add(partitionMetadata.getPartitionID());
        }
      }
      if (partitionIdsList.size() == numberOfPartitions) {
        // we have found the required number of partitions, update provisionedCapacity and
        // dedicatedPartition
        for (String partitionId : partitionIdsList) {
          PartitionMetadata newPartitionMetadata = partitionMetadataStore.getSync(partitionId);
          long provisionedCapacity =
              newPartitionMetadata.provisionedCapacity + perPartitionCapacity;
          partitionMetadataStore.updateSync(
              new PartitionMetadata(
                  partitionId,
                  provisionedCapacity,
                  newPartitionMetadata.getMaxCapacity(),
                  requireDedicatedPartition));
          await()
              .until(
                  () -> {
                    PartitionMetadata getPartitionMetadata =
                        partitionMetadataStore.getSync(partitionId);
                    return getPartitionMetadata.getProvisionedCapacity() == provisionedCapacity
                        && getPartitionMetadata.getDedicatedPartition()
                            == requireDedicatedPartition;
                  });
        }
        return partitionIdsList;
      }
    }
    // if we reached here means we did not find required number of partitions, returning empty list
    return List.of();
  }

  /**
   * fetch the latest active partition metadata update the partition provisionedCapacity and
   * dedicatedPartition status
   *
   * @param partitionIds list of partitionIds to clear
   * @param perPartitionCapacity per Partition Capacity to be cleared from partitions
   * @return map of partitionId and old partitionMetadata (to be used if reassign if failed)
   */
  public Map<String, PartitionMetadata> clearPartitions(
      List<String> partitionIds, long perPartitionCapacity) {

    Map<String, PartitionMetadata> clearedPartitions = new HashMap<>();

    for (String partitionId : partitionIds) {
      PartitionMetadata existingPartitionMetadata = partitionMetadataStore.getSync(partitionId);
      clearedPartitions.put(
          partitionId,
          new PartitionMetadata(
              partitionId,
              existingPartitionMetadata.getProvisionedCapacity(),
              existingPartitionMetadata.getMaxCapacity(),
              existingPartitionMetadata.getDedicatedPartition()));
      long provisionedCapacity =
          existingPartitionMetadata.getProvisionedCapacity() - perPartitionCapacity;

      boolean dedicatedPartition =
          provisionedCapacity != 0 && existingPartitionMetadata.dedicatedPartition;
      partitionMetadataStore.updateSync(
          new PartitionMetadata(
              partitionId,
              provisionedCapacity,
              existingPartitionMetadata.getMaxCapacity(),
              dedicatedPartition));
      await()
          .until(
              () -> {
                PartitionMetadata getPartitionMetadata =
                    partitionMetadataStore.getSync(partitionId);
                return getPartitionMetadata.getProvisionedCapacity() == provisionedCapacity
                    && getPartitionMetadata.getDedicatedPartition() == dedicatedPartition;
              });
    }
    return clearedPartitions;
  }

  /**
   * check if the current partitionIds can be reUsed to minimize the number of changes
   *
   * @param oldPartitionIds list of old partition IDs
   * @param oldThroughputBytes old throughput for which the partitions were used
   * @param newThroughputBytes new throughput for which the partitions will be used
   * @param requireDedicatedPartitions flag to indicate, if we want dedicated partition moving
   *     forward
   * @return map of partition id, oldPartitionMetada of partitions which will be re-used
   */
  public Map<String, PartitionMetadata> findPartitionsToBeReUsed(
      List<String> oldPartitionIds,
      long oldThroughputBytes,
      long newThroughputBytes,
      boolean requireDedicatedPartitions) {
    int newPartitionCount = getPartitionCount(newThroughputBytes);
    long perPartitionCapacityRequired = Math.ceilDiv(newThroughputBytes, newPartitionCount);

    int oldPartitionCount = oldPartitionIds.size();
    long perPartitionCapacityExisting = Math.ceilDiv(oldThroughputBytes, oldPartitionCount);

    Map<String, PartitionMetadata> partitionsToBeReUsed = new HashMap<>();
    for (String partitionId : oldPartitionIds) {
      PartitionMetadata partitionMetadata = partitionMetadataStore.getSync(partitionId);
      if (requireDedicatedPartitions) {
        if ((partitionMetadata.dedicatedPartition
                || partitionMetadata.getProvisionedCapacity() - perPartitionCapacityExisting == 0)
            && (partitionMetadata.getProvisionedCapacity()
                    - perPartitionCapacityExisting
                    + perPartitionCapacityRequired
                <= maxPartitionCapacity)) {
          partitionsToBeReUsed.put(
              partitionId,
              new PartitionMetadata(
                  partitionId,
                  partitionMetadata.getProvisionedCapacity(),
                  partitionMetadata.getMaxCapacity(),
                  partitionMetadata.getDedicatedPartition()));
        }
      } else {
        if ((!partitionMetadata.dedicatedPartition
                || partitionMetadata.getProvisionedCapacity() - perPartitionCapacityExisting == 0)
            && (partitionMetadata.getProvisionedCapacity()
                    - perPartitionCapacityExisting
                    + perPartitionCapacityRequired
                <= maxPartitionCapacity)) {
          partitionsToBeReUsed.put(
              partitionId,
              new PartitionMetadata(
                  partitionId,
                  partitionMetadata.getProvisionedCapacity(),
                  partitionMetadata.getMaxCapacity(),
                  partitionMetadata.getDedicatedPartition()));
        }
      }
      // we will hit this criteria when downsizing throughput
      if (partitionsToBeReUsed.size() == newPartitionCount) {
        break;
      }
    }
    for (String partitionId : partitionsToBeReUsed.keySet()) {
      PartitionMetadata existingPartitionMetadata = partitionMetadataStore.getSync(partitionId);
      long provisionedCapacity =
          existingPartitionMetadata.provisionedCapacity
              - perPartitionCapacityExisting
              + perPartitionCapacityRequired;

      partitionMetadataStore.updateSync(
          new PartitionMetadata(
              partitionId,
              provisionedCapacity,
              existingPartitionMetadata.getMaxCapacity(),
              requireDedicatedPartitions));
      await()
          .until(
              () -> {
                PartitionMetadata getPartitionMetadata =
                    partitionMetadataStore.getSync(partitionId);
                return getPartitionMetadata.getProvisionedCapacity() == provisionedCapacity
                    && getPartitionMetadata.getDedicatedPartition() == requireDedicatedPartitions;
              });
    }

    return partitionsToBeReUsed;
  }

  /**
   * perform manual partition assignment when doing update partitionAssignment
   *
   * @param datasetMetadata datasetMetadata object on which partition assignment will be done
   * @param newPartitionIdList the new PartitionId list
   * @param updatedThroughputBytes new throughput bytes
   * @param requireDedicatedPartitions if we require dedicated partition
   */
  public void manualAssignPartition(
      DatasetMetadata datasetMetadata,
      List<String> newPartitionIdList,
      long updatedThroughputBytes,
      boolean requireDedicatedPartitions) {
    List<DatasetPartitionMetadata> existingPartitions = datasetMetadata.getPartitionConfigs();
    Optional<DatasetPartitionMetadata> previousActiveDatasetPartition =
        existingPartitions.stream()
            .filter(
                datasetPartitionMetadata ->
                    datasetPartitionMetadata.getEndTimeEpochMs() == MAX_TIME)
            .findFirst();
    previousActiveDatasetPartition.ifPresent(
        datasetPartitionMetadata ->
            clearPartitions(
                datasetPartitionMetadata.getPartitions(),
                Math.ceilDiv(
                    datasetMetadata.getThroughputBytes(),
                    datasetPartitionMetadata.getPartitions().size())));
    long perPartitionCapacity = Math.ceilDiv(updatedThroughputBytes, newPartitionIdList.size());
    for (String partitionId : newPartitionIdList) {
      PartitionMetadata partitionMetadata = partitionMetadataStore.getSync(partitionId);
      long provisionedCapacity = partitionMetadata.provisionedCapacity + perPartitionCapacity;
      partitionMetadataStore.updateSync(
          new PartitionMetadata(
              partitionId,
              provisionedCapacity,
              partitionMetadata.maxCapacity,
              requireDedicatedPartitions));
    }
  }

  /**
   * automatically finds the partition Ids given requirements, with minimum swaps
   *
   * @param datasetMetadata DatasetMetadata object
   * @param throughputBytes required throughPutBytes
   * @param requireDedicatedPartition flag to indicate if we need dedicated partitions
   * @return List of partition Ids to be assigned in the new DatasetPartitionMetadata
   */
  public List<String> autoAssignPartition(
      DatasetMetadata datasetMetadata, long throughputBytes, boolean requireDedicatedPartition) {

    Optional<DatasetPartitionMetadata> previousActiveDatasetPartition =
        datasetMetadata.getPartitionConfigs().stream()
            .filter(
                datasetPartitionMetadata ->
                    datasetPartitionMetadata.getEndTimeEpochMs() == MAX_TIME)
            .findFirst();

    Map<String, PartitionMetadata> clearedPartitions = new HashMap<>();
    Map<String, PartitionMetadata> partitionsToBeResUsed;

    if (previousActiveDatasetPartition.isPresent()) {
      partitionsToBeResUsed =
          findPartitionsToBeReUsed(
              previousActiveDatasetPartition.get().getPartitions(),
              datasetMetadata.getThroughputBytes(),
              throughputBytes,
              requireDedicatedPartition);

      List<String> partitionsToBeCleared =
          previousActiveDatasetPartition.get().getPartitions().stream()
              .filter(element -> !partitionsToBeResUsed.containsKey(element))
              .collect(Collectors.toList());

      clearedPartitions =
          clearPartitions(
              partitionsToBeCleared,
              Math.ceilDiv(
                  datasetMetadata.getThroughputBytes(),
                  previousActiveDatasetPartition.get().getPartitions().size()));
    } else {
      partitionsToBeResUsed = new HashMap<>();
    }

    List<String> reUsePartitionIds = new ArrayList<>(partitionsToBeResUsed.keySet());

    List<String> newPartitionIds =
        findPartition(throughputBytes, requireDedicatedPartition, reUsePartitionIds);

    int numberOfPartitions = getPartitionCount(throughputBytes);

    // Not enough partitions are available for reassigning
    if (newPartitionIds.size() + reUsePartitionIds.size() != numberOfPartitions) {
      clearedPartitions.putAll(partitionsToBeResUsed);
      // revert the clearing operation
      for (Map.Entry<String, PartitionMetadata> entry : clearedPartitions.entrySet()) {

        partitionMetadataStore.updateSync(
            new PartitionMetadata(
                entry.getKey(),
                entry.getValue().getProvisionedCapacity(),
                entry.getValue().getMaxCapacity(),
                entry.getValue().getDedicatedPartition()));
      }
      return List.of();
    }
    return Stream.concat(reUsePartitionIds.stream(), newPartitionIds.stream())
        .collect(Collectors.toList());
  }
}
