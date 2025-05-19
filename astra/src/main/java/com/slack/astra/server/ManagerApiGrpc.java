package com.slack.astra.server;

import static com.slack.astra.metadata.dataset.DatasetMetadataSerializer.toDatasetMetadataProto;
import static com.slack.astra.metadata.partition.PartitionMetadataSerializer.toPartitionMetadataProto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.slack.astra.chunk.ChunkInfo;
import com.slack.astra.clusterManager.ReplicaRestoreService;
import com.slack.astra.metadata.core.InternalMetadataStoreException;
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

  public static final long MAX_TIME = Long.MAX_VALUE;
  // TODO: add to config
  public static final long PARTITION_START_TIME_OFFSET = 15 * 60 * 1000; // 15 minutes

  private final ReplicaRestoreService replicaRestoreService;
  private final PartitionMetadataStore partitionMetadataStore;
  private final long maxPartitionCapacity;
  private final int minNumberOfPartitions;

  public ManagerApiGrpc(
      DatasetMetadataStore datasetMetadataStore,
      PartitionMetadataStore partitionMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      ReplicaRestoreService replicaRestoreService,
      int minNumberOfPartitions,
      long maxPartitionCapacity) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.replicaRestoreService = replicaRestoreService;
    this.partitionMetadataStore = partitionMetadataStore;

    this.minNumberOfPartitions = minNumberOfPartitions;
    this.maxPartitionCapacity = maxPartitionCapacity;
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

  private List<PartitionMetadata> getCurrentPartitionMetadata() {
    return createPartitionMetadataFromDatasetConfigs().getLivePMDs();
  }

  private PartitionMetadataFromDatasetConfigs createPartitionMetadataFromDatasetConfigs() {
    return new PartitionMetadataFromDatasetConfigs(
        datasetMetadataStore.listSync(),
        partitionMetadataStore.listSync(),
        minNumberOfPartitions,
        maxPartitionCapacity);
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
        // TODO perhaps we should still check the partition capacity and warn? Or complain when
        // writing to a dedicated partition owned by another dataset?
        // manualAssignPartition
      }
      partitionIdList = partitionIdList.stream().sorted().toList();
      if (partitionIdList.isEmpty()) { // TODO not this way
        String msg = "Error updating partition assignment, could not find partitions to assign";
        LOG.error(msg);
        responseObserver.onError(Status.UNKNOWN.withDescription(msg).asException());
        return;
      }
      ImmutableList<DatasetPartitionMetadata> updatedDatasetPartitionMetadata =
          addNewPartition(
              datasetMetadata.getPartitionConfigs(),
              partitionIdList,
              request.getRequireDedicatedPartition());

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
      List<DatasetPartitionMetadata> existingPartitions,
      List<String> newPartitionIdsList,
      boolean requireDedicatedPartition) {
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
        && previousActiveDatasetPartition.get().getPartitions().equals(newPartitionIdsList)
        && previousActiveDatasetPartition.get().usesDedicatedPartition()
            == requireDedicatedPartition) {
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
    long partitionCutoverTime = Instant.now().toEpochMilli() + PARTITION_START_TIME_OFFSET;

    ImmutableList.Builder<DatasetPartitionMetadata> builder =
        ImmutableList.<DatasetPartitionMetadata>builder().addAll(remainingDatasetPartitions);

    if (previousActiveDatasetPartition.isPresent()) {
      DatasetPartitionMetadata updatedPreviousActivePartition =
          new DatasetPartitionMetadata(
              previousActiveDatasetPartition.get().getStartTimeEpochMs(),
              partitionCutoverTime,
              previousActiveDatasetPartition.get().getPartitions(),
              previousActiveDatasetPartition.get().usesDedicatedPartition());
      builder.add(updatedPreviousActivePartition);
    }

    DatasetPartitionMetadata newPartitionMetadata =
        new DatasetPartitionMetadata(
            partitionCutoverTime + 1, MAX_TIME, newPartitionIdsList, requireDedicatedPartition);
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
      try {
        PartitionMetadata partitionMetadata =
            partitionMetadataStore.getSync(request.getPartitionId());
        PartitionMetadata updatedPartitionMetadata =
            new PartitionMetadata(partitionMetadata.partitionId, 0, maxPartitionCapacity, false);
        partitionMetadataStore.updateSync(updatedPartitionMetadata);
        responseObserver.onNext(toPartitionMetadataProto(updatedPartitionMetadata));
        responseObserver.onCompleted();
      } catch (InternalMetadataStoreException e) {
        // create
        PartitionMetadata createPartitionMetadata =
            new PartitionMetadata(request.getPartitionId(), 0, maxPartitionCapacity, false);
        partitionMetadataStore.createSync(createPartitionMetadata);
        responseObserver.onNext(toPartitionMetadataProto(createPartitionMetadata));
        responseObserver.onCompleted();
      }
    } catch (StatusRuntimeException e) {
      LOG.error("Error creating new partition", e);
      responseObserver.onError(e);
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
                  getCurrentPartitionMetadata().stream()
                      .map(PartitionMetadataSerializer::toPartitionMetadataProto)
                      .toList())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error fetching partition list", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
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
    PartitionMetadataFromDatasetConfigs partitionMetadataFromDatasetConfigs =
        createPartitionMetadataFromDatasetConfigs();

    long currentThroughputBytes = datasetMetadata.getThroughputBytes();

    if (partitionMetadataFromDatasetConfigs.hasNoPartitionsDeclared()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Needed " + minNumberOfPartitions + " partitions with enough capacity, found 0")
          .asRuntimeException();
    }
    List<String> currentPartitions =
        datasetMetadata
            .getLatestPartitionMetadata()
            .map(DatasetPartitionMetadata::getPartitions)
            .orElseGet(ImmutableList::of);
    long minNumNeededPartitions =
        Math.max(
            Math.ceilDiv(
                throughputBytes, partitionMetadataFromDatasetConfigs.maxCapacityForPartitions()),
            partitionMetadataFromDatasetConfigs.minRequiredPartitions());
    long maxNumNeededPartitions =
        partitionMetadataFromDatasetConfigs.maxPartitionsUsableByDataSet();
    if (requireDedicatedPartition) {
      // if we require dedicated partitions, we can only reuse partitions that are not shared
      // approach:
      // - collect the unshared partitions for the current dataset, sorted by id
      // - collect the empty partitions, sorted by id
      // - concat those two lists, then take the first n partitions where n is the minimum needed
      //   to cover the requested throughput
      //
      // this ensures that we minimize partition churn and that we have enough partitions.
      List<String> reusablePartitions =
          partitionMetadataFromDatasetConfigs.unsharedPartitionsForDataset(
              datasetMetadata.getName());
      List<String> proposedPartitionIds =
          Stream.concat(
                  reusablePartitions.stream().sorted(),
                  partitionMetadataFromDatasetConfigs.currentEmptyPartitions().stream().sorted())
              .limit(minNumNeededPartitions)
              .toList();
      System.out.println(
          "current empty " + partitionMetadataFromDatasetConfigs.currentEmptyPartitions());
      if (proposedPartitionIds.size() < minNumNeededPartitions) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                "Needed "
                    + minNumNeededPartitions
                    + " partitions with enough capacity, found "
                    + proposedPartitionIds.size()
                    + ": "
                    + proposedPartitionIds)
            .asRuntimeException();
      }
      return proposedPartitionIds;
    } else {
      // For datasets that don't require dedicated partitions, we can use any partition
      // that has enough capacity for the new per partition throughput.
      //
      // our goals are to
      // - minimize partition churn
      // - maximize the use of existing partitions
      // so we sort them by
      //   - in current set vs not -- reuse current set first
      //   - provisioned capacity desc -- try to saturate the most provisioned partitions first
      //   - id asc -- within those, sort by id to keep order stable-ish
      // Then starting with the min number of needed partitions, we look for a set of partitions
      // with
      // enough space going up to the max number of available partitions
      List<PartitionMetadata> partitionsSorted =
          partitionMetadataFromDatasetConfigs.partitionsSortedByReusedCapacityAndId(
              currentPartitions);
      long currentPerPartitionThroughput;
      if (!currentPartitions.isEmpty()) {
        currentPerPartitionThroughput =
            Math.ceilDiv(currentThroughputBytes, currentPartitions.size());
      } else {
        currentPerPartitionThroughput = 0;
      }

      List<String> lastProposal = Collections.emptyList();
      // NB: starting at 1 so that if there is only one partition available,
      // the error message will communicate that.
      for (long proposedPartitionCt = 1;
          proposedPartitionCt <= maxNumNeededPartitions;
          proposedPartitionCt++) {
        long nextPerPartitionThroughput = Math.ceilDiv(throughputBytes, proposedPartitionCt);
        lastProposal =
            partitionsSorted.stream()
                .filter(
                    p ->
                        p.getMaxCapacity()
                            >= (p.getProvisionedCapacity() - currentPerPartitionThroughput)
                                + nextPerPartitionThroughput)
                .limit(proposedPartitionCt)
                .map(PartitionMetadata::getPartitionID)
                .toList();
        if (lastProposal.size() >= minNumNeededPartitions
            && lastProposal.size() == proposedPartitionCt) {
          return lastProposal;
        }
      }
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Needed "
                  + minNumNeededPartitions
                  + " partitions with enough capacity, found "
                  + +lastProposal.size()
                  + ": "
                  + lastProposal)
          .asRuntimeException();
    }
  }

  /**
   * Holds the result of analyzing the dataset configs and partition metadata. Used for partition
   * assigment and listing partition metadata.
   */
  private static class PartitionMetadataFromDatasetConfigs {
    private final long maxPartitionCapacity;
    private final long minNumberOfPartitions;
    private final List<String> partitionIds = new ArrayList<>();
    private final Map<String, Long> partitionProvisioning = new HashMap<>();
    private final Map<String, List<String>> partitionDatasets = new HashMap<>();
    private final Map<String, List<String>> partitionDedication = new HashMap<>();
    private final List<PartitionMetadata> livePMDs;

    public PartitionMetadataFromDatasetConfigs(
        List<DatasetMetadata> datasetMetadataList,
        List<PartitionMetadata> partitionMetadataList,
        long minNumberOfPartitions,
        long maxPartitionCapacity) {
      this.minNumberOfPartitions = minNumberOfPartitions;
      this.maxPartitionCapacity = maxPartitionCapacity;
      for (PartitionMetadata partitionMetadata : partitionMetadataList) {
        this.partitionIds.add(partitionMetadata.getPartitionID());
        this.partitionProvisioning.put(partitionMetadata.getPartitionID(), 0L);
        this.partitionDedication.put(partitionMetadata.getPartitionID(), new ArrayList<>());
        this.partitionDatasets.put(partitionMetadata.getPartitionID(), new ArrayList<>());
      }
      for (DatasetMetadata datasetMetadata : datasetMetadataList) {
        Optional<DatasetPartitionMetadata> latest = datasetMetadata.getLatestPartitionMetadata();
        long perPartitionValue =
            latest
                .map(
                    p ->
                        Math.ceilDiv(
                            datasetMetadata.getThroughputBytes(), p.getPartitions().size()))
                .orElse(0L);

        boolean useDedicatedPartition =
            latest.map(DatasetPartitionMetadata::usesDedicatedPartition).orElse(false);
        for (String partitionId :
            latest.map(DatasetPartitionMetadata::getPartitions).orElse(ImmutableList.of())) {
          this.partitionProvisioning.put(
              partitionId,
              perPartitionValue + this.partitionProvisioning.getOrDefault(partitionId, 0L));
          this.partitionDatasets.get(partitionId).add(datasetMetadata.getName());
          if (useDedicatedPartition) {
            this.partitionDedication.get(partitionId).add(datasetMetadata.getName());
          }
        }
      }
      livePMDs =
          partitionIds.stream()
              .map(
                  id ->
                      new PartitionMetadata(
                          id,
                          partitionProvisioning.get(id),
                          partitionMetadataList.stream()
                              .mapToLong(PartitionMetadata::getMaxCapacity)
                              .min()
                              .orElse(maxPartitionCapacity),
                          !partitionDedication.get(id).isEmpty()))
              .toList();
    }

    public boolean hasNoPartitionsDeclared() {
      return partitionProvisioning.isEmpty();
    }

    public long maxCapacityForPartitions() {
      return getLivePMDs().stream()
          .mapToLong(PartitionMetadata::getMaxCapacity)
          .min()
          .orElse(maxPartitionCapacity);
    }

    public List<String> currentEmptyPartitions() {
      return partitionDatasets.entrySet().stream()
          .filter(entry -> entry.getValue().isEmpty())
          .map(Map.Entry::getKey)
          .toList();
    }

    public long maxPartitionsUsableByDataSet() {
      return getLivePMDs().size();
    }

    public List<PartitionMetadata> partitionsSortedByReusedCapacityAndId(
        List<String> reusablePartitions) {
      return getLivePMDs().stream()
          .sorted(
              Comparator.comparing(
                      (PartitionMetadata p) -> reusablePartitions.contains(p.getPartitionID()))
                  .thenComparing(PartitionMetadata::getProvisionedCapacity)
                  .reversed()
                  .thenComparing(PartitionMetadata::getPartitionID))
          .toList();
    }

    public List<String> unsharedPartitionsForDataset(String datasetName) {
      return partitionDatasets.entrySet().stream()
          .filter(
              entry ->
                  entry.getValue().size() == 1 && entry.getValue().getFirst().equals(datasetName))
          .map(Map.Entry::getKey)
          .toList();
    }

    public long minRequiredPartitions() {
      return minNumberOfPartitions;
    }

    public List<PartitionMetadata> getLivePMDs() {
      return livePMDs;
    }
  }
}
