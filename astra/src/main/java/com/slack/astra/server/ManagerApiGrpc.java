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
import io.grpc.StatusRuntimeException;
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
  private final ReplicaRestoreService replicaRestoreService;

  public ManagerApiGrpc(
      DatasetMetadataStore datasetMetadataStore,
      PartitionMetadataStore partitionMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      ReplicaRestoreService replicaRestoreService) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.replicaRestoreService = replicaRestoreService;
    this.partitionMetadataStore = partitionMetadataStore;
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
      Preconditions.checkArgument(
          !request.getName().isBlank(), "Dataset name must not be blank string");

      DatasetMetadata datasetMetadata;
      try {
        datasetMetadata = datasetMetadataStore.getSync(request.getName());
      } catch (Exception e) {
        String msg = "Dataset with name, '" + request.getName() + "', does not exist";
        LOG.error(msg);
        responseObserver.onError(Status.NOT_FOUND.withDescription(msg).asException());
        return;
      }
      // if the user provided a non-negative value for throughput set it, otherwise default to the
      // existing value
      long updatedThroughputBytes =
          request.getThroughputBytes() < 0
              ? datasetMetadata.getThroughputBytes()
              : request.getThroughputBytes();

      List<String> partitionIdList;
      if (request.getPartitionIdsList().isEmpty()) {
        try {
          partitionIdList =
              autoAssignPartition(
                  datasetMetadata, updatedThroughputBytes, request.getRequireDedicatedPartition());
        } catch (StatusRuntimeException e) {
          LOG.error("Error autoassigning partitions", e);
          responseObserver.onError(e);
          return;
        }
      } else {
        partitionIdList = request.getPartitionIdsList();
        manualAssignPartition(
            datasetMetadata,
            partitionIdList,
            updatedThroughputBytes,
            request.getRequireDedicatedPartition());
      }
      if (partitionIdList.isEmpty()) { // TODO not this way
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
    } catch (IllegalArgumentException e) {
      LOG.error("Error updating partition assignment", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
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

    if (previousActiveDatasetPartition.isPresent()
        && previousActiveDatasetPartition.get().getPartitions().equals(newPartitionIdsList)) {
      return ImmutableList.copyOf(existingPartitions);
    }

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
    long partitionCutoverTime =
        Instant.now().toEpochMilli() + PartitionMetadataStore.PARTITION_START_TIME_OFFSET;

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
      long maxCapacity = partitionMetadataStore.getMaxCapacityOrDefaultTo(request.getMaxCapacity());

      long requestedProvisionedCapacity = request.getProvisionedCapacity();
      try {
        PartitionMetadata partitionMetadata =
            partitionMetadataStore.getSync(request.getPartitionId());
        // update
        long provisionedCapacity =
            PartitionMetadataStore.getProvisionedCapacity(
                requestedProvisionedCapacity, partitionMetadata);
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
                requestedProvisionedCapacity,
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
    int numberOfPartitions = partitionMetadataStore.partitionCountForThroughput(requiredThroughput);

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
        if (PartitionMetadataStore.isUnprovisioned(partitionMetadata)) {
          partitionIdsList.add(partitionMetadata.getPartitionID());
        }
      } else {
        // partition has capacity and (partition is shared or (partition not shared and
        // provisionedCapacity = 0))
        if (partitionMetadataStore.hasSpaceForAdditionalProvisioning(
                partitionMetadata, perPartitionCapacity)
            && (!partitionMetadata.dedicatedPartition
                || PartitionMetadataStore.isUnprovisioned(partitionMetadata))) {
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
   * check if the current partitionIds can be reused to minimize the number of changes
   *
   * @param oldPartitionIds list of old partition IDs
   * @param oldThroughputBytes old throughput for which the partitions were used
   * @param newThroughputBytes new throughput for which the partitions will be used
   * @param requireDedicatedPartitions flag to indicate, if we want dedicated partition moving
   *     forward
   * @return map of partition id, oldPartitionMetadata of partitions which will be re-used
   */
  public Map<String, PartitionMetadata> findPartitionsToBeReUsed(
      List<String> oldPartitionIds,
      long oldThroughputBytes,
      long newThroughputBytes,
      boolean requireDedicatedPartitions) {
    int newPartitionCount = partitionMetadataStore.partitionCountForThroughput(newThroughputBytes);
    long perPartitionCapacityRequired = Math.ceilDiv(newThroughputBytes, newPartitionCount);

    int oldPartitionCount = oldPartitionIds.size();
    long perPartitionCapacityExisting = Math.ceilDiv(oldThroughputBytes, oldPartitionCount);

    // go through the existing partitions
    // if dedicated partitions are required
    // - select partitions that are dedicated or have the same capacity usage, and can accommodate
    // the new usage
    // if dedicated partitions are not required
    // - pick partitions that are not dedicated, or are dedicated but have the same provisioning as
    // before
    //
    // what we want are existing partitions that
    Map<String, PartitionMetadata> partitionsToBeReUsed = new HashMap<>();
    for (String partitionId : oldPartitionIds) {
      PartitionMetadata partitionMetadata = partitionMetadataStore.getSync(partitionId);
      boolean provisionedCapacityMatchesExisting =
          partitionMetadata.getProvisionedCapacity() - perPartitionCapacityExisting == 0;
      boolean hasSpaceAfterRepartitioning =
          partitionMetadataStore.hasSpaceForAdditionalProvisioning(
              partitionMetadata, perPartitionCapacityRequired - perPartitionCapacityExisting);
      PartitionMetadata currentPartition =
          new PartitionMetadata(
              partitionId,
              partitionMetadata.getProvisionedCapacity(),
              partitionMetadata.getMaxCapacity(),
              partitionMetadata.getDedicatedPartition());
      if (requireDedicatedPartitions) {
        if ((partitionMetadata.dedicatedPartition || provisionedCapacityMatchesExisting)
            && hasSpaceAfterRepartitioning) {
          partitionsToBeReUsed.put(partitionId, currentPartition);
        }
      } else {
        if ((!partitionMetadata.dedicatedPartition || provisionedCapacityMatchesExisting)
            && hasSpaceAfterRepartitioning) {
          partitionsToBeReUsed.put(partitionId, currentPartition);
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
    List<String> newPartitionIds =
        proposeNewPartitions(datasetMetadata, throughputBytes, requireDedicatedPartition);
    // TODO return both new partition ids and ones to remove
    if (newPartitionIds.isEmpty()) {
      // String msg = "Error auto assigning partition, could not find partitions to assign";
      // LOG.error(msg);
      // throw new RuntimeException(msg);
      // TODO something?
      return newPartitionIds;
    }

    long newPerPartitionThroughput = throughputBytes / newPartitionIds.size();
    long previousPerPartitionThroughput =
        datasetMetadata.getPartitionConfigs().isEmpty()
            ? 0
            : datasetMetadata.getThroughputBytes() / datasetMetadata.getPartitionConfigs().size();
    for (String newPartitionId : newPartitionIds) {
      PartitionMetadata prevValue = partitionMetadataStore.getSync(newPartitionId);
      partitionMetadataStore.updateSync(
          new PartitionMetadata(
              newPartitionId,
              prevValue.getProvisionedCapacity()
                  + newPerPartitionThroughput
                  - previousPerPartitionThroughput,
              prevValue.getMaxCapacity(),
              requireDedicatedPartition));
    }
    return newPartitionIds;
  }

  private List<String> proposeNewPartitions(
      DatasetMetadata datasetMetadata, long throughputBytes, boolean requireDedicatedPartition) {
    List<PartitionMetadata> partitionMetadataList = partitionMetadataStore.listSync();
    if (partitionMetadataList.isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("no partitions to assign to")
          .asRuntimeException();
    }
    List<String> ERROR_CASE_RESULT = List.of();
    long smallestMaxCapacityForPartition =
        partitionMetadataList.stream()
            .mapToLong(PartitionMetadata::getMaxCapacity)
            .min()
            .orElseThrow();
    Optional<DatasetPartitionMetadata> currentlyActiveDatasetPartition =
        datasetMetadata.getPartitionConfigs().stream()
            .filter(
                datasetPartitionMetadata ->
                    datasetPartitionMetadata.getEndTimeEpochMs() == MAX_TIME)
            .findFirst();
    List<String> currentPartitions =
        currentlyActiveDatasetPartition
            .map(DatasetPartitionMetadata::getPartitions)
            .orElseGet(ImmutableList::of);
    List<String> currentEmptyPartitions =
        partitionMetadataList.stream()
            .filter(p -> p.getProvisionedCapacity() == 0)
            .map(PartitionMetadata::getPartitionID)
            .toList();
    List<PartitionMetadata> partitionMetadataUsedByDataset =
        partitionMetadataList.stream()
            .filter(p -> currentPartitions.contains(p.getPartitionID()))
            .toList();

    // TODO what if some are dedicated and some are shared, then ugh.
    boolean currentlyDedicatedOrEmpty =
        partitionMetadataUsedByDataset.isEmpty()
            || partitionMetadataUsedByDataset.stream()
                .allMatch(PartitionMetadata::getDedicatedPartition);
    long currentThroughputBytes = datasetMetadata.getThroughputBytes();

    List<PartitionMetadata> currentPartitionsThatHaveProvisionedCapacityBeyondCurrent =
        partitionMetadataUsedByDataset.stream()
            .filter(
                p ->
                    p.getProvisionedCapacity()
                        > currentThroughputBytes / partitionMetadataUsedByDataset.size())
            .toList();
    Set<String> currentPartitionIdsThatCannotBeReused =
        currentPartitionsThatHaveProvisionedCapacityBeyondCurrent.stream()
            .map(PartitionMetadata::getPartitionID)
            .collect(Collectors.toSet());

    // numbers for calc:
    // - current throughputBytes
    // - available throughputBytes per partition
    // - available throughputBytes (sum over partitions of p.max - p.provisioned)
    // - min available throughputBytes for partition
    // min number of partitions needed based on throughput:
    // - min of (max per partition capacity / requested throughput, 2)
    long minNumNeededPartitions =
        Math.max(
            throughputBytes / smallestMaxCapacityForPartition,
            partitionMetadataStore.minNumberOfPartitions);
    long maxNumNeededPartitions = partitionMetadataList.size();
    // - max number of partitions: largest ct of partitions where available throughput is greater
    // than requested at that ct

    if (requireDedicatedPartition) {
      List<String> reusablePartitions;
      if (!currentlyDedicatedOrEmpty
          && !currentPartitionsThatHaveProvisionedCapacityBeyondCurrent.isEmpty()) {
        // we'll need to move off of them
        reusablePartitions =
            currentPartitions.stream()
                .filter(p -> !currentPartitionIdsThatCannotBeReused.contains(p))
                .collect(Collectors.toList());
      } else {
        reusablePartitions = currentPartitions;
      }

      long numNeededPartitions = minNumNeededPartitions - reusablePartitions.size();
      List<String> partitionsToAdd;
      if (reusablePartitions.size() < minNumNeededPartitions) {
        // we need more partitions
        partitionsToAdd = currentEmptyPartitions.stream().limit(numNeededPartitions).toList();
        System.out.println("add: " + partitionsToAdd + " keep: " + reusablePartitions);
        // prepare changeset
      } else {
        // either we have exactly the right number or we have more than we need
        // TODO
        partitionsToAdd = List.of();
      }
      System.out.println("reusable" + reusablePartitions);
      System.out.println("new" + partitionsToAdd);

      List<String> proposedPartitionIds =
          Stream.concat(reusablePartitions.stream(), partitionsToAdd.stream())
              .limit(minNumNeededPartitions)
              .collect(Collectors.toList());
      System.out.println("proposed ids:" + proposedPartitionIds);
      if (proposedPartitionIds.size() < minNumNeededPartitions) {
        // we couldn't find enough partitions
        System.out.println("couldn't find enough partitions");
        return ERROR_CASE_RESULT;
        //  TODO      throw Status.FAILED_PRECONDITION.withDescription().asRuntimeException();
      }
      return proposedPartitionIds;
      // if we currently use dedicated partitions
      // - looking for existing partitions + empty partitions
      // if we currently use shared partitions
      // - if we are the only ones using the current partitions, then mark them as dedicated, then
      // do the same as the other case

    } else {
      List<String> reusablePartitions;
      if (currentlyDedicatedOrEmpty) {
        // if we currently use dedicated partitions, mark them as shared, and look for new
        // partitions to add if necessary
        // TODO
        reusablePartitions = currentPartitions;
      } else {
        reusablePartitions = currentPartitions;
      }
      // try to find a split that works, starting with the min and going to the max

      // sort partitions by provisioned capacity desc and id asc
      Map<Boolean, List<PartitionMetadata>> eitherReusableOrUnused =
          partitionMetadataList.stream()
              .sorted(
                  Comparator.comparing(PartitionMetadata::getProvisionedCapacity)
                      .reversed()
                      .thenComparing(PartitionMetadata::getPartitionID))
              .collect(
                  Collectors.partitioningBy(p -> reusablePartitions.contains(p.getPartitionID())));
      List<PartitionMetadata> reusablePartitionsList = eitherReusableOrUnused.get(true);
      List<PartitionMetadata> newPartitionsList = eitherReusableOrUnused.get(false);
      long currentPerPartitionThroughput;
      if (!currentPartitions.isEmpty()) {
        currentPerPartitionThroughput = currentThroughputBytes / currentPartitions.size();
      } else {
        currentPerPartitionThroughput = 0;
      }
      System.out.println("reusable" + reusablePartitions);
      System.out.println("new" + newPartitionsList);

      for (long proposedPartitionCt = minNumNeededPartitions;
          proposedPartitionCt <= maxNumNeededPartitions;
          proposedPartitionCt++) {
        long nextPerPartitionThroughput = throughputBytes / proposedPartitionCt;
        Stream<PartitionMetadata> reusablePartitionsWithEnoughSpace =
            reusablePartitionsList.stream()
                .filter(
                    p ->
                        p.getMaxCapacity()
                                - (p.getProvisionedCapacity() - currentPerPartitionThroughput)
                                - nextPerPartitionThroughput
                            >= 0);
        Stream<PartitionMetadata> newPartitionsWithEnoughSpace =
            newPartitionsList.stream()
                .filter(
                    p ->
                        p.getMaxCapacity() - p.getProvisionedCapacity() - nextPerPartitionThroughput
                            >= 0);
        List<PartitionMetadata> proposedReuseablePartitionList =
            reusablePartitionsList.stream()
                .filter(
                    p ->
                        p.getMaxCapacity()
                                - (p.getProvisionedCapacity() - currentPerPartitionThroughput)
                                - nextPerPartitionThroughput
                            >= 0)
                .limit(proposedPartitionCt)
                .toList();
        List<PartitionMetadata> proposedNewPartitionList =
            newPartitionsList.stream()
                .filter(
                    p ->
                        p.getMaxCapacity() - p.getProvisionedCapacity() - nextPerPartitionThroughput
                            >= 0)
                .limit(proposedPartitionCt - proposedReuseablePartitionList.size())
                .toList();
        List<PartitionMetadata> proposedPartitions =
            Stream.concat(reusablePartitionsWithEnoughSpace, newPartitionsWithEnoughSpace)
                .limit(proposedPartitionCt)
                .toList();
        System.out.println("proposed" + proposedPartitions);
        System.out.println(
            "proposed change: add: "
                + proposedNewPartitionList
                + " keep: "
                + proposedReuseablePartitionList);
        if (proposedNewPartitionList.size() + reusablePartitionsList.size()
            >= minNumNeededPartitions) {
          System.out.println("proposed change: " + proposedPartitions);
          return proposedPartitions.stream().map(PartitionMetadata::getPartitionID).toList();
        } else {
          System.out.println(
              "proposedPartitionct: " + proposedPartitionCt + " couldn't make a proposal");
        }
      }
      throw Status.FAILED_PRECONDITION
          .withDescription("not enough partitions with sufficient unprovisioned capacity")
          .asRuntimeException();
    }
  }
  // for each proposed partition
  //    partitionMetadataStore.updateSync(
  //      new PartitionMetadata(
  //        partitionId,
  //        provisionedCapacity,
  //        newPartitionMetadata.getMaxCapacity(),
  //        requireDedicatedPartition));
  // for each cleared partition (means I need to track them too)
  // do the thing
  // return list
  // or maybe we defer that to the caller?
  // that way we can dry run it more easily
  // yeah I think I would prefer that.
  /*
  public List<String> whatever() {
    Optional<DatasetPartitionMetadata> previousActiveDatasetPartition;
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

    int numberOfPartitions = partitionMetadataStore.partitionCountForThroughput(throughputBytes);

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
      //      return List.of();
      throw new RuntimeException(
          "not enough partition space: new: "
              + newPartitionIds.size()
              + " old:"
              + reUsePartitionIds.size()
              + "!= needed: "
              + numberOfPartitions
              + " cleared "
              + clearedPartitions);
    }
    return Stream.concat(reUsePartitionIds.stream(), newPartitionIds.stream())
        .collect(Collectors.toList());
  }
  */
}
