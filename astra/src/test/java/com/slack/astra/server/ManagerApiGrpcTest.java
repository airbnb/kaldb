package com.slack.astra.server;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.astra.clusterManager.ReplicaRestoreService;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.core.InternalMetadataStoreException;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.manager_api.ManagerApi;
import com.slack.astra.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.MetricsUtil;
import com.slack.astra.util.GrpcCleanupExtension;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ManagerApiGrpcTest {

  public static final int DEFAULT_MAX_CAPACITY = 5000000;
  @RegisterExtension public final GrpcCleanupExtension grpcCleanup = new GrpcCleanupExtension();

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;
  private DatasetMetadataStore datasetMetadataStore;
  private PartitionMetadataStore partitionMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;
  private ReplicaRestoreService replicaRestoreService;
  private ManagerApiServiceGrpc.ManagerApiServiceBlockingStub managerApiStub;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ManagerApiGrpcTest")
            .setZkSessionTimeoutMs(30000)
            .setZkConnectionTimeoutMs(30000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    datasetMetadataStore = spy(new DatasetMetadataStore(curatorFramework, true));
    partitionMetadataStore =
        spy(new PartitionMetadataStore(curatorFramework, true, DEFAULT_MAX_CAPACITY, 2));
    snapshotMetadataStore = spy(new SnapshotMetadataStore(curatorFramework));
    replicaMetadataStore = spy(new ReplicaMetadataStore(curatorFramework));

    AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRecreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1"))
            .setMaxReplicasPerRequest(200)
            .setReplicaLifespanMins(60)
            .setSchedulePeriodMins(30)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaRestoreServiceConfig(replicaRecreationServiceConfig)
            .build();

    replicaRestoreService =
        new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);

    grpcCleanup.register(
        InProcessServerBuilder.forName(this.getClass().toString())
            .directExecutor()
            .addService(
                new ManagerApiGrpc(
                    datasetMetadataStore,
                    partitionMetadataStore,
                    snapshotMetadataStore,
                    replicaRestoreService))
            .build()
            .start());
    ManagedChannel channel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(this.getClass().toString()).directExecutor().build());

    managerApiStub = ManagerApiServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  public void tearDown() throws Exception {
    replicaRestoreService.stopAsync();
    replicaRestoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    replicaMetadataStore.close();
    snapshotMetadataStore.close();
    datasetMetadataStore.close();
    partitionMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldCreateAndGetNewDataset() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner)
            .build());

    Metadata.DatasetMetadata getDatasetMetadataResponse = getDatasetMetadataGRPC(datasetName);
    assertThat(getDatasetMetadataResponse.getName()).isEqualTo(datasetName);
    assertThat(getDatasetMetadataResponse.getOwner()).isEqualTo(datasetOwner);
    assertThat(getDatasetMetadataResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(getDatasetMetadataResponse.getPartitionConfigsList().size()).isEqualTo(0);

    DatasetMetadata datasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(datasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(datasetMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorCreatingDuplicateDatasetName() {
    String datasetName = "testDataset";
    String datasetOwner1 = "testOwner1";
    String datasetOwner2 = "testOwner2";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner1)
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner2)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    DatasetMetadata datasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(datasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.getOwner()).isEqualTo(datasetOwner1);
    assertThat(datasetMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorCreatingWithInvalidDatasetNames() {
    String datasetOwner = "testOwner";

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName("")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getStatus().getDescription()).isEqualTo("name can't be null or empty.");

    StatusRuntimeException throwable2 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName("/")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable2.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    StatusRuntimeException throwable3 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(".")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable3.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorWithEmptyOwnerInformation() {
    String datasetName = "testDataset";

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner("")
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable.getStatus().getDescription()).isEqualTo("owner must not be null or blank");

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldUpdateExistingDataset() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    String serviceNamePattern = "serviceNamePattern";
    String updatedServiceNamePattern = "updatedServiceNamePattern";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner)
            .setServiceNamePattern(serviceNamePattern)
            .build());

    String updatedDatasetOwner = "testOwnerUpdated";
    Metadata.DatasetMetadata updatedDatasetResponse =
        managerApiStub.updateDatasetMetadata(
            ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(updatedDatasetOwner)
                .setServiceNamePattern(serviceNamePattern)
                .build());

    assertThat(updatedDatasetResponse.getName()).isEqualTo(datasetName);
    assertThat(updatedDatasetResponse.getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(updatedDatasetResponse.getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(updatedDatasetResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(updatedDatasetResponse.getPartitionConfigsList().size()).isEqualTo(0);

    AtomicReference<DatasetMetadata> datasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().getOwner().equals(updatedDatasetOwner);
            });

    assertThat(datasetMetadata.get().getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.get().getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(datasetMetadata.get().getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(datasetMetadata.get().getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.get().getPartitionConfigs().size()).isEqualTo(0);

    Metadata.DatasetMetadata updatedServiceNamePatternResponse =
        managerApiStub.updateDatasetMetadata(
            ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(updatedDatasetOwner)
                .setServiceNamePattern(updatedServiceNamePattern)
                .build());

    assertThat(updatedServiceNamePatternResponse.getName()).isEqualTo(datasetName);
    assertThat(updatedServiceNamePatternResponse.getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(updatedServiceNamePatternResponse.getServiceNamePattern())
        .isEqualTo(updatedServiceNamePattern);
    assertThat(updatedServiceNamePatternResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(updatedServiceNamePatternResponse.getPartitionConfigsList().size()).isEqualTo(0);

    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return Objects.equals(
                  datasetMetadata.get().getServiceNamePattern(), updatedServiceNamePattern);
            });

    datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
    assertThat(datasetMetadata.get().getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.get().getServiceNamePattern()).isEqualTo(updatedServiceNamePattern);
    assertThat(datasetMetadata.get().getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(datasetMetadata.get().getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.get().getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorGettingNonexistentDataset() {
    StatusRuntimeException throwable =
        (StatusRuntimeException) catchThrowable(() -> getDatasetMetadataGRPC("foo"));
    Status status = throwable.getStatus();
    assertThat(status.getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldUpdatePartitionManualAssignments() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    partitionMetadataStore.createSync(createSharedPartitionMetadata("1"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("2"));

    Metadata.DatasetMetadata initialDatasetRequest =
        createEmptyDatasetGRPC(datasetName, datasetOwner);
    assertThat(initialDatasetRequest.getPartitionConfigsList().size()).isEqualTo(0);

    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = 10;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .addAllPartitionIds(List.of("1", "2"))
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 1
                    && datasetMetadataStore.listSync().get(0).getThroughputBytes()
                        == throughputBytes);

    Metadata.DatasetMetadata firstAssignment = getDatasetMetadataGRPC(datasetName);

    assertThat(firstAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstAssignment.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(getPartitionMetadata("1", "2")).have(sharedPartitionsWithCapacity(5));

    DatasetMetadata firstDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(firstDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(firstDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(firstDatasetMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstDatasetMetadata.getPartitionConfigs().size()).isEqualTo(1);

    partitionMetadataStore.createSync(createSharedPartitionMetadata("3"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("4"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("5"));

    // only update the partition assignment, leaving throughput
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(-1)
            .addAllPartitionIds(List.of("3", "4", "5"))
            .build());

    AtomicReference<Metadata.DatasetMetadata> secondAssignment = new AtomicReference<>();
    await()
        .until(
            () -> {
              secondAssignment.set(getDatasetMetadataGRPC(datasetName));
              return secondAssignment.get().getThroughputBytes() == throughputBytes;
            });

    assertThat(secondAssignment.get().getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(secondAssignment.get().getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(secondAssignment.get().getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(secondAssignment.get().getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(getPartitionMetadata("1", "2")).have(sharedPartitionsWithCapacity(0));
    assertThat(getPartitionMetadata("3", "4", "5")).have(sharedPartitionsWithCapacity(4));

    DatasetMetadata secondDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(secondDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(secondDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(secondDatasetMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(secondDatasetMetadata.getPartitionConfigs().size()).isEqualTo(2);

    // only update the throughput, leaving the partition assignments (we need to re-specify to avoid
    // auto-assignment)
    long updatedThroughputBytes = 12;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(updatedThroughputBytes)
            .addAllPartitionIds(List.of("3", "4", "5"))
            .build());

    AtomicReference<Metadata.DatasetMetadata> thirdAssignment = new AtomicReference<>();
    await()
        .until(
            () -> {
              thirdAssignment.set(getDatasetMetadataGRPC(datasetName));
              return thirdAssignment.get().getThroughputBytes() == updatedThroughputBytes;
            });

    assertThat(thirdAssignment.get().getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdAssignment.get().getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(getPartitionMetadata("1", "2")).have(sharedPartitionsWithCapacity(0));
    assertThat(getPartitionMetadata("3", "4", "5")).have(sharedPartitionsWithCapacity(4));

    DatasetMetadata thirdDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(thirdDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(thirdDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(thirdDatasetMetadata.getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdDatasetMetadata.getPartitionConfigs().size()).isEqualTo(2);
  }

  // auto-assignment cases
  // properties
  // partition space
  // - count: 0, 1, 2, 3+
  // - enough capacity available: yes, yes but needs reassignment, no
  // datasets
  // - no other ones besides the one being created
  // - existing shared datasets
  // - existing dedicated datasets
  // - a mix of shared and dedicated datasets
  //
  // - number of datasets
  // [- adding a dataset]
  // [- removing a dataset]
  // [- updating a dataset]
  // -- shared
  // --- updating but the existing dataset partitions still have capacity available
  // --- updating and the existing dataset partitions are full
  // --- other partitions have capacity? y/n
  // -- dedicated
  // --- updating but the existing dataset partitions still have capacity available
  // --- updating and the existing dataset partitions are full
  // - one dataset
  // --
  // not enough partitions, add partitions, see that the update worked
  // manual assign
  // 1. updating empty dataset
  // 2. updating existing dataset where there are partitions in common (I think this is currently
  // broken)
  // cases where shared/dedicated doesn't matter
  // 1. dataset to update with blank name  -> error
  // 1. dataset to update doesn't exist  -> error
  // 2. no existing partitions, create dataset and assign partitions -> error
  // 3. not enough partitions, create dataset and assign partitions -> error
  // adding shared
  // 1. happy path, 0 existing datasets, 5 partitions, all empty
  // 2. 1 existing shared dataset, 2 partitions, both used but with enough space
  // 3. 1 existing shared dataset, 3 partitions, 2 used, one free, but not enough space without
  // reassigning -> say that there's capacity perhaps, but would need to change other dataset
  // configs
  // 4. 1 existing shared dataset, 3 partitions, 3 used, enough space if assigned to 3 but not
  // enough if assigned to 2
  // 5. 1 existing dedicated dataset, 3 partitions, 2 used, one free -> error (space doesn't matter
  // because can't have 2 partitions for both)
  // 6. 1 existing dedicated dataset, 4 partitions, 2 used, 2 free -> success
  // because can have 2 partitions for both)
  //
  // adding dedicated
  // 1. happy path. no existing datasets, 5 partitions setup
  // 2. 1 existing dedicated dataset, 3 partitions, 2 used, one free -> error (space doesn't matter)
  // 3. 1 existing dedicated dataset, 4 partitions, 2 used, 2 free
  // 4. 1 existing dedicated dataset, 4 partitions, all used but could be reassigned based on
  // throughput amounts requested
  // 5. 1 existing dedicated dataset, 4 partitions, 3 used 1 free but could be reassigned based on
  // throughput amounts requested
  // 6. 1 existing dedicated dataset, 4 partitions, 3 used 1 free but not enough space even with
  // reassignment based on throughput amounts requested
  // 7. 1 existing shared dataset, 3 partitions, 2 used, one free -> error (space doesn't matter
  // because can't have 2 partitions for both)
  // updating
  // 1. switch from shared to dedicated partitions, 0 other datasets, enough space available
  // 1. switch from shared to dedicated partitions, 1 other datasets, enough empty partitions
  // available
  // 2. switch from shared to dedicated partitions, 1 other datasets, not enough space available
  // -- shared partitions with other dataset
  // -- shared partitions with no other dataset
  // -- shared partitions some with other dataset some with just the one under test
  //
  // 3. switch from dedicated to shared partitions
  // -- this is easy unless you want to rebalance onto the other shared partitions to make space,
  // but tbh that should be
  // -- a different operation
  //
  // - increasing the throughput of a dataset. shared vs dedicated
  // - switching from shared to dedicated for a dataset
  // - switching from dedicated to shared for a dataset
  // - 4 partitions, 2 used, 2 empty, adding a shared dataset, should it use the empty ones first or
  // the shared ones?
  // I'd expect empty
  // - it'd be nice if there were errors that differentiated between needing rebalancing and needing
  // more partitions
  // though that's not strictly necessary
  // - invalid starting configurations and how it handles that
  //
  // what are the not enough space cases
  // - no partitions at all (should only happen the first time?)
  // - not enough empty partitions
  // - not enough space available total

  @Test
  public void shouldAutoAssignAddShared1() {
    // setup
    // - 5 partitions
    // - no existing datasets
    // action
    // - create an empty dataset
    // - update its partitions with some throughputs
    // should have at least 2 partitions assigned to it
    // the partition metadata should reflect the throughput
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    createPartitions("1", "2", "3", "4", "5");
    createEmptyDatasetGRPC(datasetName, datasetOwner);

    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = 10;
    ManagerApi.UpdatePartitionAssignmentResponse updateResponse =
        updatePartitionAssignmentGRPC(datasetName, throughputBytes);
    assertThat(updateResponse.getAssignedPartitionIdsList()).isEqualTo(List.of("1", "2"));
    await().until(() -> datasetHasPartitionConfigAfterTime(datasetName, nowMs));

    // assert that there are >= 2 partitions assigned to the dataset
    assertThat(
            latestPartitionConfig(datasetMetadataStore.getSync(datasetName)).getPartitions().size())
        .isEqualTo(2);
    // assert that the partition metadata has the throughput for those partitions
    assertThat(getPartitionMetadata("1", "2"))
        .have(sharedPartitionsWithCapacity(throughputBytes / 2));
    // assert that the other partitions are not updated
    assertThat(getPartitionMetadata("3", "4", "5")).have(sharedPartitionsWithCapacity(0));
  }

  @Test
  public void shouldAutoAssignAddShared2() {
    // 2. 1 existing shared dataset, 2 partitions, both used but with enough space
    // setup
    // - 2 partitions
    // - 1 existing datasets

    int existingDatasetThroughputBytes = 1_000;
    List<String> existingPartitions = List.of("1", "2");
    datasetMetadataStore.createAsync(
        new DatasetMetadata(
            "existingDataset1",
            "existingDatasetOwner",
            existingDatasetThroughputBytes,
            List.of(
                new DatasetPartitionMetadata(
                    Instant.now().toEpochMilli(), MAX_TIME, existingPartitions)),
            "whatever"));
    createPartitions(existingPartitions, existingDatasetThroughputBytes / 2);
    // action
    // - create an empty dataset
    // - update its partitions with some throughputs
    // should have at least 2 partitions assigned to it
    // the partition metadata should reflect the throughput
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    createEmptyDatasetGRPC(datasetName, datasetOwner);

    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = 1_000;
    ManagerApi.UpdatePartitionAssignmentResponse updateResponse =
        updatePartitionAssignmentGRPC(datasetName, throughputBytes);
    assertThat(updateResponse.getAssignedPartitionIdsList()).isEqualTo(List.of("1", "2"));
    await().until(() -> datasetHasPartitionConfigAfterTime(datasetName, nowMs));

    // assert that there are >= 2 partitions assigned to the dataset
    assertThat(
            latestPartitionConfig(datasetMetadataStore.getSync(datasetName)).getPartitions().size())
        .isEqualTo(2);
    // assert that the partition metadata has the throughput for those partitions
    assertThat(getPartitionMetadata("1", "2"))
        .are(
            sharedPartitionsWithCapacity(throughputBytes / 2 + existingDatasetThroughputBytes / 2));
  }

  @Test
  public void shouldAutoAssignAddShared3() {
    // 3. 1 existing shared dataset, 3 partitions, 2 used, one free, but not enough space without
    // reassigning
    // setup
    // - 3 partitions, 2 used, one free
    // - 1 existing datasets

    // existing using max capacity * 2 - 1 on two partitions
    // requesting max capacity * 1 - 1
    // would require the existing dataset to be reassigned to all three partitions
    int existingDatasetThroughputBytes = DEFAULT_MAX_CAPACITY * 2 - 2;
    List<String> usedExistingPartitions = List.of("1", "2");
    datasetMetadataStore.createAsync(
        new DatasetMetadata(
            "existingDataset1",
            "existingDatasetOwner",
            existingDatasetThroughputBytes,
            List.of(
                new DatasetPartitionMetadata(
                    Instant.now().toEpochMilli(), MAX_TIME, usedExistingPartitions)),
            "whatever"));
    createPartitions(usedExistingPartitions, existingDatasetThroughputBytes / 2);
    createPartitions("3");
    // action
    // - create an empty dataset
    // - update its partitions with some throughputs
    // should have at least 2 partitions assigned to it
    // the partition metadata should reflect the throughput
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    createEmptyDatasetGRPC(datasetName, datasetOwner);

    long throughputBytes = DEFAULT_MAX_CAPACITY;
    assertThatThrownBy(() -> updatePartitionAssignmentGRPC(datasetName, throughputBytes))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            withGrpcStatusAndDescription(
                Status.FAILED_PRECONDITION,
                "not enough partitions with sufficient unprovisioned capacity"));
  }

  @Test
  public void shouldAutoAssignAddShared4() {
    // 4. 1 existing shared dataset, 3 partitions, 3 used, enough space if assigned to 3 but not
    // enough if assigned to 2
    // setup
    // - 3 partitions, 3 used
    // - 1 existing datasets
    int existingDatasetThroughputBytes = DEFAULT_MAX_CAPACITY * 2 - 2;
    List<String> usedExistingPartitions = List.of("1", "2", "3");
    datasetMetadataStore.createAsync(
        new DatasetMetadata(
            "existingDataset1",
            "existingDatasetOwner",
            existingDatasetThroughputBytes,
            List.of(
                new DatasetPartitionMetadata(
                    Instant.now().toEpochMilli(), MAX_TIME, usedExistingPartitions)),
            "whatever"));
    createPartitions(
        usedExistingPartitions, existingDatasetThroughputBytes / usedExistingPartitions.size());
    // action
    // - create an empty dataset
    // - update its partitions with some throughputs
    // should have at least 2 partitions assigned to it
    // the partition metadata should reflect the throughput
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    createEmptyDatasetGRPC(datasetName, datasetOwner);

    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = DEFAULT_MAX_CAPACITY;
    ManagerApi.UpdatePartitionAssignmentResponse updateResponse =
        updatePartitionAssignmentGRPC(datasetName, throughputBytes);
    assertThat(updateResponse.getAssignedPartitionIdsList()).isEqualTo(List.of("1", "2", "3"));
    await().until(() -> datasetHasPartitionConfigAfterTime(datasetName, nowMs));

    // assert that there are >= 2 partitions assigned to the dataset
    assertThat(
            latestPartitionConfig(datasetMetadataStore.getSync(datasetName)).getPartitions().size())
        .isEqualTo(3);
    // assert that the partition metadata has the throughput for those partitions
    assertThat(getPartitionMetadata("1", "2", "3"))
        .are(
            sharedPartitionsWithCapacity(throughputBytes / 3 + existingDatasetThroughputBytes / 3));
  }

  @Test
  public void shouldAutoAssignAddShared5() {
    // 5. 1 existing dedicated dataset, 3 partitions, 2 used, one free -> error (space doesn't
    // matter
    // because can't have 2 partitions for both)
    // setup
    // - 3 partitions, 2 used, one free
    // - 1 existing datasets

    // existing using max capacity * 2 - 2 on three partitions, which should leave a third for the
    // new dataset
    // requesting max capacity * 1 - 1
    // would require the existing dataset to be reassigned to all three partitions
    int existingDatasetThroughputBytes = DEFAULT_MAX_CAPACITY * 2 - 2;
    List<String> usedExistingPartitions = List.of("1", "2");
    datasetMetadataStore.createAsync(
        new DatasetMetadata(
            "existingDataset1",
            "existingDatasetOwner",
            existingDatasetThroughputBytes,
            List.of(
                new DatasetPartitionMetadata(
                    Instant.now().toEpochMilli(), MAX_TIME, usedExistingPartitions)),
            "whatever"));
    createDedicatedPartitions(
        usedExistingPartitions, existingDatasetThroughputBytes / usedExistingPartitions.size());
    // action
    // - create an empty dataset
    // - update its partitions with some throughputs
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    createEmptyDatasetGRPC(datasetName, datasetOwner);

    assertThatThrownBy(() -> updatePartitionAssignmentGRPC(datasetName, DEFAULT_MAX_CAPACITY))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            withGrpcStatusAndDescription(
                Status.FAILED_PRECONDITION,
                "not enough partitions with sufficient unprovisioned capacity"));
  }

  @Test
  public void shouldAutoAssignAddShared6() {
    // 6. 1 existing dedicated dataset, 4 partitions, 2 used, 2 free -> success
    // because can't have 2 partitions for both)
    int existingDatasetThroughputBytes = DEFAULT_MAX_CAPACITY * 2 - 2;
    List<String> usedExistingPartitions = List.of("1", "2");
    List<String> unusedExistingPartitions = List.of("3", "4");
    datasetMetadataStore.createAsync(
        new DatasetMetadata(
            "existingDataset1",
            "existingDatasetOwner",
            existingDatasetThroughputBytes,
            List.of(
                new DatasetPartitionMetadata(
                    Instant.now().toEpochMilli(), MAX_TIME, usedExistingPartitions)),
            "whatever"));
    createDedicatedPartitions(
        usedExistingPartitions, existingDatasetThroughputBytes / usedExistingPartitions.size());
    createPartitions(unusedExistingPartitions.toArray(new String[0]));
    // action
    // - create an empty dataset
    // - update its partitions with some throughputs
    // should have at least 2 partitions assigned to it
    // the partition metadata should reflect the throughput
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    createEmptyDatasetGRPC(datasetName, datasetOwner);

    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = DEFAULT_MAX_CAPACITY;
    ManagerApi.UpdatePartitionAssignmentResponse updateResponse =
        updatePartitionAssignmentGRPC(datasetName, throughputBytes);
    assertThat(updateResponse.getAssignedPartitionIdsList()).isEqualTo(unusedExistingPartitions);
    await().until(() -> datasetHasPartitionConfigAfterTime(datasetName, nowMs));

    // assert that there are >= 2 partitions assigned to the dataset
    assertThat(
            latestPartitionConfig(datasetMetadataStore.getSync(datasetName)).getPartitions().size())
        .isEqualTo(2);
    // assert that the partition metadata has the throughput for those partitions
    long expectedNewProvisionedCapacity = throughputBytes / 2;
    assertThat(getPartitionMetadata("1", "2"))
        .are(dedicatedPartitionsWithCapacity(existingDatasetThroughputBytes / 2));
    assertThat(getPartitionMetadata("3", "4"))
        .have(sharedPartitionsWithCapacity(expectedNewProvisionedCapacity));
  }

  private Condition<? super PartitionMetadata> dedicatedPartitionsWithCapacity(
      long provisionedCapacity) {
    return partitionsWithCapacityAndDedicated(provisionedCapacity, true);
  }

  private Condition<? super PartitionMetadata> sharedPartitionsWithCapacity(
      long provisionedCapacity) {
    return partitionsWithCapacityAndDedicated(provisionedCapacity, false);
  }

  private Condition<? super PartitionMetadata> partitionsWithCapacityAndDedicated(
      long provisionedCapacity, boolean dedicated) {
    return new Condition<>(
        p -> p.getProvisionedCapacity() == provisionedCapacity && p.dedicatedPartition == dedicated,
        (dedicated ? "dedicated" : "shared") + " partition with capacity " + provisionedCapacity);
  }

  private List<PartitionMetadata> getPartitionMetadata(String... partitionIds) {
    return Arrays.stream(partitionIds).map(partitionMetadataStore::getSync).toList();
  }

  private void createPartitions(List<String> usedExistingPartitions, int provisionedCapacity) {
    for (String number : usedExistingPartitions) {
      partitionMetadataStore.createSync(createSharedPartitionMetadata(number, provisionedCapacity));
    }
  }

  private void createDedicatedPartitions(
      List<String> usedExistingPartitions, int provisionedCapacity) {
    for (String number : usedExistingPartitions) {
      partitionMetadataStore.createSync(
          createDedicatedPartitionMetadata(number, provisionedCapacity));
    }
  }

  @Test
  public void shouldAutoAssign2NoPartitions() {
    // setup
    // - no partitions
    // - create empty dataset
    // action
    // - update dataset
    // expect error of type what?
    // error should say what to do or at least hint at it?
    createEmptyDatasetGRPC("testDataset", "testOwner");
    assertThatThrownBy(() -> updatePartitionAssignmentGRPC("testDataset", 10))
        .isInstanceOfSatisfying(
            io.grpc.StatusRuntimeException.class,
            withGrpcStatusAndDescription(
                Status.FAILED_PRECONDITION,
                "no partitions to assign to")); // TODO better error message
  }

  @Test
  public void shouldAutoAssign3NotEnoughPartitions() {
    // setup
    // - 1 partitions
    // - create empty dataset
    // action
    // - update dataset
    // expect error of type what?
    // error should say what to do or at least hint at it?
    createEmptyDatasetGRPC("testDataset", "testOwner");
    createPartitions("1");

    assertThatThrownBy(() -> updatePartitionAssignmentGRPC("testDataset", 10))
        .isInstanceOfSatisfying(
            io.grpc.StatusRuntimeException.class,
            withGrpcStatusAndDescription(
                Status.FAILED_PRECONDITION,
                "not enough partitions with sufficient unprovisioned capacity"));
  }

  @Test
  public void shouldInvalidArgumentOnUpdatingWithBlankDataset() {
    assertThatThrownBy(() -> updatePartitionAssignmentGRPC("", 10))
        .isInstanceOfSatisfying(
            io.grpc.StatusRuntimeException.class,
            withGrpcStatusAndDescription(
                Status.INVALID_ARGUMENT, "Dataset name must not be blank string"));
  }

  private static Consumer<StatusRuntimeException> withGrpcStatusAndDescription(
      Status expectedStatus, String expectedDescription) {
    return e -> {
      assertThat(e.getStatus().getCode()).isEqualTo(expectedStatus.getCode());
      assertThat(e.getStatus().getDescription()).isEqualTo(expectedDescription);
    };
  }

  private ManagerApi.UpdatePartitionAssignmentResponse updatePartitionAssignmentGRPC(
      String datasetName, long throughputBytes) {
    return managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .build());
  }

  private DatasetPartitionMetadata latestPartitionConfig(DatasetMetadata datasetMetadata) {
    return datasetMetadata.getPartitionConfigs().stream()
        .filter(p -> p.getEndTimeEpochMs() == MAX_TIME)
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No partition configs found"));
  }

  private boolean datasetHasPartitionConfigAfterTime(String datasetName, long nowMs) {
    return datasetMetadataStore.listSync().stream()
        .anyMatch(
            d ->
                d.getName().equals(datasetName)
                    && d.getPartitionConfigs().stream()
                        .anyMatch(p -> p.getStartTimeEpochMs() >= nowMs));
  }

  private Metadata.DatasetMetadata getDatasetMetadataGRPC(String datasetName) {
    return managerApiStub.getDatasetMetadata(
        ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());
  }

  private Metadata.DatasetMetadata createEmptyDatasetGRPC(String datasetName, String datasetOwner) {
    Metadata.DatasetMetadata initialDatasetRequest =
        managerApiStub.createDatasetMetadata(
            ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(datasetOwner)
                .build());
    return initialDatasetRequest;
  }

  private void createPartitions(String... numbers) {
    for (String number : numbers) {
      partitionMetadataStore.createSync(createSharedPartitionMetadata(number));
    }
  }

  @Test
  public void shouldUpdatePartitionAutoAssignmentsSingleDatasetMetadata() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    createPartitions("1", "2", "3", "4", "5");

    Metadata.DatasetMetadata initialDatasetRequest =
        createEmptyDatasetGRPC(datasetName, datasetOwner);
    assertThat(initialDatasetRequest.getPartitionConfigsList().size()).isEqualTo(0);

    // first assignment: partitionList empty, ThroughputBytes provided, requireDedicatedPartition =
    // False
    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = 10;
    ManagerApi.UpdatePartitionAssignmentRequest request =
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .build();
    managerApiStub.updatePartitionAssignment(request);
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 1
                    && datasetMetadataStore.listSync().get(0).getPartitionConfigs().size() == 1);

    Metadata.DatasetMetadata firstAssignment = getDatasetMetadataGRPC(datasetName);

    assertThat(firstAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstAssignment.getPartitionConfigsList().size()).isEqualTo(1);
    Metadata.DatasetPartitionMetadata firstPartitionConfig =
        firstAssignment.getPartitionConfigsList().get(0);
    assertThat(firstPartitionConfig.getPartitionsList()).isEqualTo(List.of("1", "2"));
    assertThat(firstPartitionConfig.getStartTimeEpochMs()).isGreaterThanOrEqualTo(nowMs);
    assertThat(firstPartitionConfig.getEndTimeEpochMs()).isEqualTo(MAX_TIME);

    assertThat(getPartitionMetadata("1", "2")).have(sharedPartitionsWithCapacity(5));

    DatasetMetadata firstDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(firstDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(firstDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(firstDatasetMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstDatasetMetadata.getPartitionConfigs().size()).isEqualTo(1);

    // second assignment: partitionList empty, ThroughputBytes not provided,
    // requireDedicatedPartition = True
    nowMs = Instant.now().toEpochMilli();
    throughputBytes = -1;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .setRequireDedicatedPartition(true)
            .build());
    await()
        .until(
            () -> {
              System.out.println(datasetMetadataStore.listSync().size());
              if (datasetMetadataStore.listSync().size() == 1) {
                System.out.println(
                    datasetMetadataStore.listSync().get(0).getPartitionConfigs().size());
              }
              return datasetMetadataStore.listSync().size() == 1
                  && datasetMetadataStore.listSync().get(0).getPartitionConfigs().size() == 1;
            });

    Metadata.DatasetMetadata secondAssignment = getDatasetMetadataGRPC(datasetName);

    assertThat(secondAssignment.getThroughputBytes()).isEqualTo(10);
    assertThat(secondAssignment.getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(secondAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(secondAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);
    assertThat(secondAssignment.getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(secondAssignment.getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(secondAssignment.getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(getPartitionMetadata("1", "2")).have(dedicatedPartitionsWithCapacity(5));

    DatasetMetadata secondDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(secondDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(secondDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(secondDatasetMetadata.getThroughputBytes()).isEqualTo(10);
    assertThat(secondDatasetMetadata.getPartitionConfigs().size()).isEqualTo(2);

    // third assignment: partitionList given, ThroughputBytes provided, requireDedicatedPartition =
    // False
    nowMs = Instant.now().toEpochMilli();
    throughputBytes = 12;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .addAllPartitionIds(List.of("3", "4", "5"))
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 1
                    && datasetMetadataStore.listSync().get(0).getPartitionConfigs().size() == 3);

    Metadata.DatasetMetadata thirdAssignment = getDatasetMetadataGRPC(datasetName);

    assertThat(thirdAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(thirdAssignment.getPartitionConfigsList().size()).isEqualTo(3);
    assertThat(thirdAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(thirdAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);
    assertThat(thirdAssignment.getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(thirdAssignment.getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(thirdAssignment.getPartitionConfigsList().get(2).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(thirdAssignment.getPartitionConfigsList().get(2).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(thirdAssignment.getPartitionConfigsList().get(2).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(getPartitionMetadata("1", "2")).have(sharedPartitionsWithCapacity(0));
    assertThat(getPartitionMetadata("3", "4", "5")).have(sharedPartitionsWithCapacity(4));

    DatasetMetadata thirdDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(thirdDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(thirdDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(thirdDatasetMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(thirdDatasetMetadata.getPartitionConfigs().size()).isEqualTo(3);

    // fourth assignment: move back to auto-assignment, also decrease in partitions
    nowMs = Instant.now().toEpochMilli();
    throughputBytes = -1;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 1
                    && datasetMetadataStore.listSync().get(0).getPartitionConfigs().size() == 4);

    Metadata.DatasetMetadata fourthAssignment = getDatasetMetadataGRPC(datasetName);

    assertThat(fourthAssignment.getThroughputBytes()).isEqualTo(12);
    assertThat(fourthAssignment.getPartitionConfigsList().size()).isEqualTo(4);
    assertThat(fourthAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(fourthAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);
    assertThat(fourthAssignment.getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(fourthAssignment.getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(fourthAssignment.getPartitionConfigsList().get(2).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(fourthAssignment.getPartitionConfigsList().get(2).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(fourthAssignment.getPartitionConfigsList().get(3).getPartitionsList())
        .isEqualTo(List.of("3", "4"));
    assertThat(fourthAssignment.getPartitionConfigsList().get(3).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(fourthAssignment.getPartitionConfigsList().get(3).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(getPartitionMetadata("1", "2")).have(sharedPartitionsWithCapacity(0));
    assertThat(getPartitionMetadata("3", "4")).have(sharedPartitionsWithCapacity(6));
    assertThat(getPartitionMetadata("5")).have(sharedPartitionsWithCapacity(0));

    DatasetMetadata fourthDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(fourthDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(fourthDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(fourthDatasetMetadata.getThroughputBytes()).isEqualTo(12);
    assertThat(fourthDatasetMetadata.getPartitionConfigs().size()).isEqualTo(4);
  }

  @Test
  public void shouldUpdatePartitionAutoAssignmentsMultipleDatasetMetadata() {
    String datasetName1 = "testDataset1";
    String datasetOwner1 = "testOwner1";
    String datasetName2 = "testDataset2";
    String datasetOwner2 = "testOwner2";

    createPartitions("1", "2", "3", "4", "5", "6");

    Metadata.DatasetMetadata initialDatasetRequest1 =
        createEmptyDatasetGRPC(datasetName1, datasetOwner1);
    assertThat(initialDatasetRequest1.getPartitionConfigsList().size()).isEqualTo(0);

    Metadata.DatasetMetadata initialDatasetRequest2 =
        createEmptyDatasetGRPC(datasetName2, datasetOwner2);
    assertThat(initialDatasetRequest2.getPartitionConfigsList().size()).isEqualTo(0);

    // first assignment: partitionList empty, ThroughputBytes provided
    long nowMs = Instant.now().toEpochMilli();
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName1)
            .setThroughputBytes(12000000)
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName2)
            .setThroughputBytes(2000000)
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 2
                    && datasetMetadataStore.listSync().get(0).getPartitionConfigs().size() == 1
                    && datasetMetadataStore.listSync().get(1).getPartitionConfigs().size() == 1);

    assertThat(getDatasetMetadataGRPC(datasetName1))
        .has(throughputBytesOf(12000000))
        .has(partitionConfigsSizeOf(1))
        .has(partitionsForIndexOf(0, List.of("1", "2", "3")))
        .has(latestPartitionConfigWithIndexOf(0, nowMs));

    assertThat(getDatasetMetadataGRPC(datasetName2))
        .has(throughputBytesOf(2000000))
        .has(partitionConfigsSizeOf(1))
        .has(partitionsForIndexOf(0, List.of("1", "2")))
        .has(latestPartitionConfigWithIndexOf(0, nowMs));

    assertThat(getPartitionMetadata("1", "2")).have(sharedPartitionsWithCapacity(5000000));
    assertThat(getPartitionMetadata("3")).have(sharedPartitionsWithCapacity(4000000));

    // second Assignment: move dataset 1 to dedicated partitions, require dedicated partitions =
    // True
    nowMs = Instant.now().toEpochMilli();
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName1)
            .setThroughputBytes(-1)
            .setRequireDedicatedPartition(true)
            .build());

    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 2
                    && datasetMetadataStore.getSync(datasetName1).getPartitionConfigs().size() == 2
                    && datasetMetadataStore.getSync(datasetName2).getPartitionConfigs().size()
                        == 1);

    assertThat(getDatasetMetadataGRPC(datasetName1))
        .has(throughputBytesOf(12000000))
        .has(partitionConfigsSizeOf(2))
        .has(partitionsForIndexOf(0, List.of("1", "2", "3")))
        .has(partitionsForIndexOf(1, List.of("3", "4", "5")))
        .has(oldPartitionConfigWithIndexOf(0))
        .has(latestPartitionConfigWithIndexOf(1, nowMs));

    assertThat(getDatasetMetadataGRPC(datasetName2))
        .has(throughputBytesOf(2000000))
        .has(partitionConfigsSizeOf(1))
        .has(partitionsForIndexOf(0, List.of("1", "2")))
        .has(latestPartitionConfigWithIndexOf(0, nowMs));

    //    Metadata.DatasetMetadata secondAssignment1 = getDatasetMetadataGRPC(datasetName1);
    //    Metadata.DatasetMetadata secondAssignment2 = getDatasetMetadataGRPC(datasetName2);
    //
    //    assertThat(secondAssignment1.getThroughputBytes()).isEqualTo(12000000);
    //    assertThat(secondAssignment1.getPartitionConfigsList().size()).isEqualTo(2);
    //    assertThat(secondAssignment1.getPartitionConfigsList().get(0).getPartitionsList())
    //        .isEqualTo(List.of("1", "2", "3"));
    //    assertThat(secondAssignment1.getPartitionConfigsList().get(0).getEndTimeEpochMs())
    //        .isNotEqualTo(MAX_TIME);
    //
    //    assertThat(secondAssignment1.getPartitionConfigsList().get(1).getPartitionsList())
    //        .isEqualTo(List.of("3", "4", "5"));
    //    assertThat(secondAssignment1.getPartitionConfigsList().get(1).getStartTimeEpochMs())
    //        .isGreaterThanOrEqualTo(nowMs);
    //    assertThat(secondAssignment1.getPartitionConfigsList().get(1).getEndTimeEpochMs())
    //        .isEqualTo(MAX_TIME);
    //
    //    assertThat(secondAssignment2.getThroughputBytes()).isEqualTo(2000000);
    //    assertThat(secondAssignment2.getPartitionConfigsList().size()).isEqualTo(1);
    //    assertThat(secondAssignment2.getPartitionConfigsList().get(0).getPartitionsList())
    //        .isEqualTo(List.of("1", "2"));
    //    assertThat(secondAssignment2.getPartitionConfigsList().get(0).getStartTimeEpochMs())
    //        .isGreaterThanOrEqualTo(nowMs);
    //    assertThat(secondAssignment2.getPartitionConfigsList().get(0).getEndTimeEpochMs())
    //        .isEqualTo(MAX_TIME);

    assertThat(getPartitionMetadata("1", "2")).have(dedicatedPartitionsWithCapacity(1000000));
    assertThat(getPartitionMetadata("3", "4", "5")).have(dedicatedPartitionsWithCapacity(4000000));

    // third Assignment: move dataset 1 to shared partitions, decrease throughput to 8000000
    nowMs = Instant.now().toEpochMilli();
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName1)
            .setThroughputBytes(8000000)
            .setRequireDedicatedPartition(false)
            .build());

    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 2
                    && datasetMetadataStore.getSync(datasetName1).getPartitionConfigs().size() == 3
                    && datasetMetadataStore.getSync(datasetName2).getPartitionConfigs().size()
                        == 1);

    assertThat(getDatasetMetadataGRPC(datasetName1))
        .has(throughputBytesOf(8000000))
        .has(partitionConfigsSizeOf(3))
        .has(partitionsForIndexOf(0, List.of("1", "2", "3")))
        .has(partitionsForIndexOf(1, List.of("3", "4", "5")))
        .has(partitionsForIndexOf(2, List.of("3", "4")))
        .has(oldPartitionConfigWithIndexOf(0))
        .has(oldPartitionConfigWithIndexOf(1))
        .has(latestPartitionConfigWithIndexOf(2, nowMs));

    // unchanged
    assertThat(getDatasetMetadataGRPC(datasetName2))
        .has(throughputBytesOf(2000000))
        .has(partitionConfigsSizeOf(1))
        .has(partitionsForIndexOf(0, List.of("1", "2")))
        .has(latestPartitionConfigWithIndexOf(0, nowMs));

    assertThat(getPartitionMetadata("1", "2")).have(sharedPartitionsWithCapacity(1000000));
    assertThat(getPartitionMetadata("3", "4")).have(sharedPartitionsWithCapacity(4000000));
    assertThat(getPartitionMetadata("5")).have(sharedPartitionsWithCapacity(0));

    // fourth Assignment: move dataset 1 to shared partitions, increase throughput to 12000000
    nowMs = Instant.now().toEpochMilli();
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName1)
            .setThroughputBytes(12000000)
            .setRequireDedicatedPartition(false)
            .build());

    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 2
                    && datasetMetadataStore.getSync(datasetName1).getPartitionConfigs().size() == 4
                    && datasetMetadataStore.getSync(datasetName2).getPartitionConfigs().size()
                        == 1);

    assertThat(getDatasetMetadataGRPC(datasetName1))
        .has(throughputBytesOf(12000000))
        .has(partitionConfigsSizeOf(4))
        .has(partitionsForIndexOf(0, List.of("1", "2", "3")))
        .has(partitionsForIndexOf(1, List.of("3", "4", "5")))
        .has(partitionsForIndexOf(2, List.of("3", "4")))
        .has(partitionsForIndexOf(3, List.of("3", "4", "1")))
        .has(oldPartitionConfigWithIndexOf(0))
        .has(oldPartitionConfigWithIndexOf(1))
        .has(oldPartitionConfigWithIndexOf(2))
        .has(latestPartitionConfigWithIndexOf(3, nowMs));

    // unchanged
    assertThat(getDatasetMetadataGRPC(datasetName2))
        .has(throughputBytesOf(2000000))
        .has(partitionConfigsSizeOf(1))
        .has(partitionsForIndexOf(0, List.of("1", "2")))
        .has(latestPartitionConfigWithIndexOf(0, nowMs));

    assertThat(getPartitionMetadata("1")).have(sharedPartitionsWithCapacity(5000000));
    assertThat(getPartitionMetadata("2")).have(sharedPartitionsWithCapacity(1000000));
    assertThat(getPartitionMetadata("3", "4")).have(sharedPartitionsWithCapacity(4000000));
    assertThat(getPartitionMetadata("5")).have(sharedPartitionsWithCapacity(0));
  }

  private static Condition<Metadata.DatasetMetadata> latestPartitionConfigWithIndexOf(
      int partitionConfigIndex, long nowMs) {
    return new Condition<>(
        d ->
            d.getPartitionConfigsList().get(partitionConfigIndex).getEndTimeEpochMs() == MAX_TIME
                && d.getPartitionConfigsList().get(partitionConfigIndex).getStartTimeEpochMs()
                    >= nowMs,
        "partition config at index %s is latest (endtime is max and start is after now)");
  }

  private static Condition<Metadata.DatasetMetadata> oldPartitionConfigWithIndexOf(
      int partitionConfigIndex) {
    return new Condition<>(
        d -> d.getPartitionConfigsList().get(partitionConfigIndex).getEndTimeEpochMs() < MAX_TIME,
        "partition config at index %s is not latest (endtime is less than max)");
  }

  private static Condition<Metadata.DatasetMetadata> partitionsForIndexOf(
      int partitionConfigIndex, List<String> expectedPartitions) {
    return new Condition<>(
        d ->
            d.getPartitionConfigsList()
                .get(partitionConfigIndex)
                .getPartitionsList()
                .equals(expectedPartitions),
        "partitionConfigsList %s of %s",
        partitionConfigIndex,
        expectedPartitions);
  }

  private static Condition<Metadata.DatasetMetadata> partitionConfigsSizeOf(
      int expectedPartitionConfigSize) {
    return new Condition<>(
        d -> d.getPartitionConfigsList().size() == expectedPartitionConfigSize,
        "partitionConfigsList size of %s",
        expectedPartitionConfigSize);
  }

  private static Condition<Metadata.DatasetMetadata> throughputBytesOf(int expectedThroughput) {
    return new Condition<>(
        d -> d.getThroughputBytes() == expectedThroughput,
        "throughputBytes should be %s",
        expectedThroughput);
  }

  private static PartitionMetadata createDedicatedPartitionMetadata(
      String partitionNumber, long provisionedCapacity) {
    return new PartitionMetadata(partitionNumber, provisionedCapacity, DEFAULT_MAX_CAPACITY, true);
  }

  private static PartitionMetadata createSharedPartitionMetadata(
      String partitionNumber, long provisionedCapacity) {
    return new PartitionMetadata(partitionNumber, provisionedCapacity, DEFAULT_MAX_CAPACITY, false);
  }

  private static PartitionMetadata createSharedPartitionMetadata(String partitionNumber) {
    return new PartitionMetadata(partitionNumber, 0, DEFAULT_MAX_CAPACITY, false);
  }

  @Test
  public void shouldErrorUpdatingPartitionAutoAssignmentNotEnoughPartitions() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    createPartitions("1", "2");

    Metadata.DatasetMetadata initialDatasetRequest =
        createEmptyDatasetGRPC(datasetName, datasetOwner);
    assertThat(initialDatasetRequest.getPartitionConfigsList()).isEmpty();

    assertThatThrownBy(
            () ->
                managerApiStub.updatePartitionAssignment(
                    ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                        .setName(datasetName)
                        .setThroughputBytes(15000000)
                        .build()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            withGrpcStatusAndDescription(
                Status.FAILED_PRECONDITION,
                "not enough partitions with sufficient unprovisioned capacity"));

    assertThat(getDatasetMetadataGRPC(datasetName).getPartitionConfigsList()).isEmpty();

    assertThat(partitionMetadataStore.getSync("1")).isEqualTo(createSharedPartitionMetadata("1"));
    assertThat(partitionMetadataStore.getSync("2")).isEqualTo(createSharedPartitionMetadata("2"));
  }

  @Test
  public void shouldErrorUpdatingPartitionAssignmentNonexistentDataset() {
    String datasetName = "testDataset";
    List<String> partitionList = List.of("1", "2");

    assertThatThrownBy(
            () ->
                managerApiStub.updatePartitionAssignment(
                    ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                        .setName(datasetName)
                        .setThroughputBytes(-1)
                        .addAllPartitionIds(partitionList)
                        .build()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            withGrpcStatusAndDescription(
                Status.NOT_FOUND, "Dataset with name, '" + datasetName + "', does not exist"));

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldListExistingDatasets() {
    String datasetName1 = "testDataset1";
    String datasetOwner1 = "testOwner1";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName1)
            .setOwner(datasetOwner1)
            .build());

    String datasetName2 = "testDataset2";
    String datasetOwner2 = "testOwner2";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName2)
            .setOwner(datasetOwner2)
            .build());

    ManagerApi.ListDatasetMetadataResponse listDatasetMetadataResponse =
        managerApiStub.listDatasetMetadata(
            ManagerApi.ListDatasetMetadataRequest.newBuilder().build());

    assertThat(
        listDatasetMetadataResponse
            .getDatasetMetadataList()
            .containsAll(
                List.of(
                    Metadata.DatasetMetadata.newBuilder()
                        .setName(datasetName1)
                        .setOwner(datasetOwner1)
                        .setThroughputBytes(0)
                        .build(),
                    Metadata.DatasetMetadata.newBuilder()
                        .setName(datasetName2)
                        .setOwner(datasetOwner2)
                        .setThroughputBytes(0)
                        .build())));

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(2);
    assertThat(
        AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore)
            .containsAll(
                List.of(
                    new DatasetMetadata(
                        datasetName1, datasetOwner1, 0, Collections.emptyList(), datasetName1),
                    new DatasetMetadata(
                        datasetName2, datasetOwner2, 0, Collections.emptyList(), datasetName2))));
  }

  @Test
  public void shouldHandleZkErrorsGracefully() {
    String datasetName = "testZkErrorsDataset";
    String datasetOwner = "testZkErrorsOwner";
    String errorString = "zkError";

    doThrow(new InternalMetadataStoreException(errorString))
        .when(datasetMetadataStore)
        .createSync(
            eq(
                new DatasetMetadata(
                    datasetName, datasetOwner, 0L, Collections.emptyList(), datasetName)));

    StatusRuntimeException throwableCreate =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner)
                            .setServiceNamePattern(datasetName)
                            .build()));

    assertThat(throwableCreate.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwableCreate.getStatus().getDescription()).isEqualTo(errorString);

    doThrow(new InternalMetadataStoreException(errorString))
        .when(datasetMetadataStore)
        .updateSync(
            eq(
                new DatasetMetadata(
                    datasetName, datasetOwner, 0L, Collections.emptyList(), datasetName)));

    StatusRuntimeException throwableUpdate =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updateDatasetMetadata(
                        ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner)
                            .build()));

    assertThat(throwableUpdate.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwableUpdate.getStatus().getDescription()).contains(datasetName);

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldFetchSnapshotsWithinTimeframeAndPartition() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata overlapsStartTimeIncluded =
        new SnapshotMetadata("a", startTime, startTime + 6, 0, "a", 0);
    SnapshotMetadata overlapsStartTimeExcluded =
        new SnapshotMetadata("b", startTime, startTime + 6, 0, "b", 0);

    SnapshotMetadata fullyOverlapsStartEndTimeIncluded =
        new SnapshotMetadata("c", startTime + 4, startTime + 11, 0, "a", 0);
    SnapshotMetadata fullyOverlapsStartEndTimeExcluded =
        new SnapshotMetadata("d", startTime + 4, startTime + 11, 0, "b", 0);

    SnapshotMetadata partiallyOverlapsStartEndTimeIncluded =
        new SnapshotMetadata("e", startTime + 4, startTime + 5, 0, "a", 0);
    SnapshotMetadata partiallyOverlapsStartEndTimeExcluded =
        new SnapshotMetadata("f", startTime + 4, startTime + 5, 0, "b", 0);

    SnapshotMetadata overlapsEndTimeIncluded =
        new SnapshotMetadata("g", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata overlapsEndTimeExcluded =
        new SnapshotMetadata("h", startTime + 10, startTime + 15, 0, "b", 0);

    SnapshotMetadata notWithinStartEndTimeExcluded1 =
        new SnapshotMetadata("i", startTime, startTime + 4, 0, "a", 0);
    SnapshotMetadata notWithinStartEndTimeExcluded2 =
        new SnapshotMetadata("j", startTime + 11, startTime + 15, 0, "a", 0);

    DatasetMetadata datasetWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a"))),
            "fooService");

    datasetMetadataStore.createSync(datasetWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    List<SnapshotMetadata> snapshotsWithData =
        ManagerApiGrpc.calculateRequiredSnapshots(
            Arrays.asList(
                overlapsEndTimeIncluded,
                overlapsEndTimeExcluded,
                partiallyOverlapsStartEndTimeIncluded,
                partiallyOverlapsStartEndTimeExcluded,
                fullyOverlapsStartEndTimeIncluded,
                fullyOverlapsStartEndTimeExcluded,
                overlapsStartTimeIncluded,
                overlapsStartTimeExcluded,
                notWithinStartEndTimeExcluded1,
                notWithinStartEndTimeExcluded2),
            datasetMetadataStore,
            start,
            end,
            "foo");

    assertThat(snapshotsWithData.size()).isEqualTo(4);
    assertThat(
            snapshotsWithData.containsAll(
                Arrays.asList(
                    overlapsStartTimeIncluded,
                    fullyOverlapsStartEndTimeIncluded,
                    partiallyOverlapsStartEndTimeIncluded,
                    overlapsEndTimeIncluded)))
        .isTrue();
  }

  @Test
  public void shouldRestoreReplicaSinglePartition() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata snapshotIncluded =
        new SnapshotMetadata("g", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotExcluded =
        new SnapshotMetadata("h", startTime + 10, startTime + 15, 0, "b", 0);

    snapshotMetadataStore.createSync(snapshotIncluded);
    snapshotMetadataStore.createSync(snapshotExcluded);

    DatasetMetadata serviceWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a"))),
            "fooService");

    datasetMetadataStore.createSync(serviceWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);
    await().until(() -> snapshotMetadataStore.listSync().size() == 2);

    managerApiStub.restoreReplica(
        ManagerApi.RestoreReplicaRequest.newBuilder()
            .setServiceName("foo")
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await()
        .until(
            () -> MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry) == 1);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);
  }

  @Test
  public void shouldRestoreReplicasMultiplePartitions() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata snapshotIncluded =
        new SnapshotMetadata("a", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotIncluded2 =
        new SnapshotMetadata("b", startTime + 10, startTime + 15, 0, "b", 0);
    SnapshotMetadata snapshotExcluded =
        new SnapshotMetadata("c", startTime + 10, startTime + 15, 0, "c", 0);

    snapshotMetadataStore.createSync(snapshotIncluded);
    snapshotMetadataStore.createSync(snapshotIncluded2);
    snapshotMetadataStore.createSync(snapshotExcluded);

    DatasetMetadata serviceWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a", "b"))),
            "fooService");

    datasetMetadataStore.createSync(serviceWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);
    await().until(() -> snapshotMetadataStore.listSync().size() == 3);

    replicaRestoreService.startAsync();
    replicaRestoreService.awaitRunning();

    managerApiStub.restoreReplica(
        ManagerApi.RestoreReplicaRequest.newBuilder()
            .setServiceName("foo")
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(2);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);

    replicaRestoreService.stopAsync();
  }

  @Test
  public void shouldRestoreGivenSnapshotIds() {
    long startTime = Instant.now().toEpochMilli();

    SnapshotMetadata snapshotFoo =
        new SnapshotMetadata("foo", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotBar =
        new SnapshotMetadata("bar", startTime + 10, startTime + 15, 0, "b", 0);
    SnapshotMetadata snapshotBaz =
        new SnapshotMetadata("baz", startTime + 10, startTime + 15, 0, "c", 0);

    snapshotMetadataStore.createSync(snapshotFoo);
    snapshotMetadataStore.createSync(snapshotBar);
    snapshotMetadataStore.createSync(snapshotBaz);
    await().until(() -> snapshotMetadataStore.listSync().size() == 3);

    replicaRestoreService.startAsync();
    replicaRestoreService.awaitRunning();

    managerApiStub.restoreReplicaIds(
        ManagerApi.RestoreReplicaIdsRequest.newBuilder()
            .addAllIdsToRestore(List.of("foo", "bar", "baz"))
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 3);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(3);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);

    replicaRestoreService.stopAsync();
  }

  @Test
  public void shouldCreateAndGetNewPartitionOnlyPartitionId() {
    // a newly created partition should be returned from the API and be in the metadata store
    String partitionId = "1";

    Metadata.PartitionMetadata createdPartition =
        managerApiStub.createPartition(
            ManagerApi.CreatePartitionRequest.newBuilder().setPartitionId(partitionId).build());

    assertThat(createdPartition.getPartitionId()).isEqualTo(partitionId);
    assertThat(createdPartition.getProvisionedCapacity()).isEqualTo(0);
    assertThat(createdPartition.getMaxCapacity()).isEqualTo(5000000);
    assertThat(createdPartition.getDedicatedPartition()).isEqualTo(false);

    assertThat(partitionMetadataStore.getSync(partitionId))
        .isEqualTo(createSharedPartitionMetadata(partitionId));
  }

  @Test
  public void shouldCreateAndGetNewPartition() {
    String partitionId = "1";
    long provisionedCapacity = 1000000;
    boolean dedicatedPartition = false;

    Metadata.PartitionMetadata createdPartition =
        managerApiStub.createPartition(
            ManagerApi.CreatePartitionRequest.newBuilder()
                .setPartitionId(partitionId)
                .setProvisionedCapacity(provisionedCapacity)
                .setDedicatedPartition(dedicatedPartition)
                .build());

    assertThat(createdPartition.getPartitionId()).isEqualTo(partitionId);
    assertThat(createdPartition.getProvisionedCapacity()).isEqualTo(provisionedCapacity);
    assertThat(createdPartition.getMaxCapacity()).isEqualTo(DEFAULT_MAX_CAPACITY);
    assertThat(createdPartition.getDedicatedPartition()).isEqualTo(dedicatedPartition);

    PartitionMetadata partitionMetadata = partitionMetadataStore.getSync(partitionId);
    assertThat(partitionMetadata)
        .isEqualTo(createSharedPartitionMetadata(partitionId, provisionedCapacity));
  }

  @Test
  public void shouldUpdateExistingPartition() {
    partitionMetadataStore.createSync(createSharedPartitionMetadata("1", 1000000));

    Metadata.PartitionMetadata partitionMetadata =
        managerApiStub.createPartition(
            ManagerApi.CreatePartitionRequest.newBuilder()
                .setPartitionId("1")
                .setDedicatedPartition(true)
                .build());

    assertThat(partitionMetadata.getPartitionId()).isEqualTo("1");
    assertThat(partitionMetadata.getProvisionedCapacity()).isEqualTo(1000000);
    assertThat(partitionMetadata.getDedicatedPartition()).isEqualTo(true);
  }

  @Test
  public void shouldListPartition() {
    partitionMetadataStore.createSync(createSharedPartitionMetadata("1"));
    partitionMetadataStore.createSync(createDedicatedPartitionMetadata("2", 0));

    Metadata.ListPartitionMetadataResponse listPartitionMetadataResponse =
        managerApiStub.listPartition(ManagerApi.ListPartitionRequest.newBuilder().build());

    assertThat(listPartitionMetadataResponse.getPartitionMetadataList().size()).isEqualTo(2);
  }

  @Test
  public void shouldDeleteDedicatedPartitionDatasetMetadata() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    String datasetServicePattern = "testDataset";

    partitionMetadataStore.createSync(createDedicatedPartitionMetadata("1", 500000));
    partitionMetadataStore.createSync(createDedicatedPartitionMetadata("2", 500000));

    datasetMetadataStore.createSync(
        new DatasetMetadata(
            datasetName,
            datasetOwner,
            1000000,
            List.of(
                new DatasetPartitionMetadata(
                    Instant.now().toEpochMilli(), MAX_TIME, List.of("1", "2"))),
            datasetServicePattern));
    managerApiStub.deleteDatasetMetadata(
        ManagerApi.DeleteDatasetMetadataRequest.newBuilder().setName(datasetName).build());

    PartitionMetadata partitionMetadata1 = partitionMetadataStore.getSync("1");
    assertThat(partitionMetadata1.getProvisionedCapacity()).isEqualTo(0);
    assertThat(partitionMetadata1.getDedicatedPartition()).isEqualTo(false);

    PartitionMetadata partitionMetadata2 = partitionMetadataStore.getSync("2");
    assertThat(partitionMetadata2.getProvisionedCapacity()).isEqualTo(0);
    assertThat(partitionMetadata2.getDedicatedPartition()).isEqualTo(false);
  }

  @Test
  public void shouldDeleteSharedPartitionDatasetMetadata() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    String datasetServicePattern = "testDataset";

    partitionMetadataStore.createSync(createSharedPartitionMetadata("1", 4000000));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("2", 4000000));
    datasetMetadataStore.createSync(
        new DatasetMetadata(
            datasetName,
            datasetOwner,
            6000000,
            List.of(
                new DatasetPartitionMetadata(
                    Instant.now().toEpochMilli(), MAX_TIME, List.of("1", "2"))),
            datasetServicePattern));

    managerApiStub.deleteDatasetMetadata(
        ManagerApi.DeleteDatasetMetadataRequest.newBuilder().setName(datasetName).build());

    PartitionMetadata partitionMetadata1 = partitionMetadataStore.getSync("1");
    assertThat(partitionMetadata1.getProvisionedCapacity()).isEqualTo(1000000);
    assertThat(partitionMetadata1.getDedicatedPartition()).isEqualTo(false);

    PartitionMetadata partitionMetadata2 = partitionMetadataStore.getSync("2");
    assertThat(partitionMetadata2.getProvisionedCapacity()).isEqualTo(1000000);
    assertThat(partitionMetadata2.getDedicatedPartition()).isEqualTo(false);
  }

  @Test
  public void shouldErrorDeletingNonExistentDatasetMetadata() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    String datasetServicePattern = "testDataset";

    partitionMetadataStore.createSync(createDedicatedPartitionMetadata("1", 4000000));
    partitionMetadataStore.createSync(createDedicatedPartitionMetadata("2", 4000000));

    datasetMetadataStore.createSync(
        new DatasetMetadata(
            datasetName,
            datasetOwner,
            6000000,
            List.of(
                new DatasetPartitionMetadata(
                    Instant.now().toEpochMilli(), MAX_TIME, List.of("1", "2"))),
            datasetServicePattern));

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.deleteDatasetMetadata(
                        ManagerApi.DeleteDatasetMetadataRequest.newBuilder()
                            .setName("testDataset1")
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    PartitionMetadata partitionMetadata1 = partitionMetadataStore.getSync("1");
    assertThat(partitionMetadata1.getProvisionedCapacity()).isEqualTo(4000000);
    assertThat(partitionMetadata1.getDedicatedPartition()).isEqualTo(true);

    PartitionMetadata partitionMetadata2 = partitionMetadataStore.getSync("2");
    assertThat(partitionMetadata2.getProvisionedCapacity()).isEqualTo(4000000);
    assertThat(partitionMetadata2.getDedicatedPartition()).isEqualTo(true);
  }
}
