package com.slack.astra.server;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
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
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
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

    Metadata.DatasetMetadata getDatasetMetadataResponse =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());
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
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.getDatasetMetadata(
                        ManagerApi.GetDatasetMetadataRequest.newBuilder().setName("foo").build()));
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
        managerApiStub.createDatasetMetadata(
            ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(datasetOwner)
                .build());
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

    Metadata.DatasetMetadata firstAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

    assertThat(firstAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstAssignment.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 5));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 5));

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
              secondAssignment.set(
                  managerApiStub.getDatasetMetadata(
                      ManagerApi.GetDatasetMetadataRequest.newBuilder()
                          .setName(datasetName)
                          .build()));
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

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 0));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 0));
    assertThat(partitionMetadataStore.getSync("3"))
        .isEqualTo(createSharedPartitionMetadata("3", 4));
    assertThat(partitionMetadataStore.getSync("4"))
        .isEqualTo(createSharedPartitionMetadata("4", 4));
    assertThat(partitionMetadataStore.getSync("5"))
        .isEqualTo(createSharedPartitionMetadata("5", 4));

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
              thirdAssignment.set(
                  managerApiStub.getDatasetMetadata(
                      ManagerApi.GetDatasetMetadataRequest.newBuilder()
                          .setName(datasetName)
                          .build()));
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

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 0));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 0));
    assertThat(partitionMetadataStore.getSync("3"))
        .isEqualTo(createSharedPartitionMetadata("3", 4));
    assertThat(partitionMetadataStore.getSync("4"))
        .isEqualTo(createSharedPartitionMetadata("4", 4));
    assertThat(partitionMetadataStore.getSync("5"))
        .isEqualTo(createSharedPartitionMetadata("5", 4));

    DatasetMetadata thirdDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(thirdDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(thirdDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(thirdDatasetMetadata.getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdDatasetMetadata.getPartitionConfigs().size()).isEqualTo(2);
  }

  @Test
  public void shouldUpdatePartitionAutoAssignmentsSingleDatasetMetadata() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    partitionMetadataStore.createSync(createSharedPartitionMetadata("1"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("2"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("3"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("4"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("5"));

    Metadata.DatasetMetadata initialDatasetRequest =
        managerApiStub.createDatasetMetadata(
            ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(datasetOwner)
                .build());
    assertThat(initialDatasetRequest.getPartitionConfigsList().size()).isEqualTo(0);

    // first assignment: partitionList empty, ThroughputBytes provided, requireDedicatedPartition =
    // False
    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = 10;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 1
                    && datasetMetadataStore.listSync().get(0).getPartitionConfigs().size() == 1);

    Metadata.DatasetMetadata firstAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

    assertThat(firstAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstAssignment.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    PartitionMetadata partitionMetadata1 = partitionMetadataStore.getSync("1");
    assertThat(partitionMetadata1.getProvisionedCapacity()).isEqualTo(5);
    assertThat(partitionMetadata1.getDedicatedPartition()).isEqualTo(false);
    PartitionMetadata partitionMetadata2 = partitionMetadataStore.getSync("2");
    assertThat(partitionMetadata2.getProvisionedCapacity()).isEqualTo(5);
    assertThat(partitionMetadata2.getDedicatedPartition()).isEqualTo(false);

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
            () ->
                datasetMetadataStore.listSync().size() == 1
                    && datasetMetadataStore.listSync().get(0).getPartitionConfigs().size() == 2);

    Metadata.DatasetMetadata secondAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

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

    partitionMetadata1 = partitionMetadataStore.getSync("1");
    assertThat(partitionMetadata1.getProvisionedCapacity()).isEqualTo(5);
    assertThat(partitionMetadata1.getDedicatedPartition()).isEqualTo(true);
    partitionMetadata2 = partitionMetadataStore.getSync("2");
    assertThat(partitionMetadata2.getProvisionedCapacity()).isEqualTo(5);
    assertThat(partitionMetadata2.getDedicatedPartition()).isEqualTo(true);

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

    Metadata.DatasetMetadata thirdAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

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

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 0));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 0));
    assertThat(partitionMetadataStore.getSync("3"))
        .isEqualTo(createSharedPartitionMetadata("3", 4));
    assertThat(partitionMetadataStore.getSync("4"))
        .isEqualTo(createSharedPartitionMetadata("4", 4));
    assertThat(partitionMetadataStore.getSync("5"))
        .isEqualTo(createSharedPartitionMetadata("5", 4));

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

    Metadata.DatasetMetadata fourthAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

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

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 0));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 0));
    assertThat(partitionMetadataStore.getSync("3"))
        .isEqualTo(createSharedPartitionMetadata("3", 6));
    assertThat(partitionMetadataStore.getSync("4"))
        .isEqualTo(createSharedPartitionMetadata("4", 6));
    assertThat(partitionMetadataStore.getSync("5"))
        .isEqualTo(createSharedPartitionMetadata("5", 0));

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

    partitionMetadataStore.createSync(createSharedPartitionMetadata("1"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("2"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("3"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("4"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("5"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("6"));

    Metadata.DatasetMetadata initialDatasetRequest1 =
        managerApiStub.createDatasetMetadata(
            ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                .setName(datasetName1)
                .setOwner(datasetOwner1)
                .build());
    assertThat(initialDatasetRequest1.getPartitionConfigsList().size()).isEqualTo(0);

    Metadata.DatasetMetadata initialDatasetRequest2 =
        managerApiStub.createDatasetMetadata(
            ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                .setName(datasetName2)
                .setOwner(datasetOwner2)
                .build());
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

    Metadata.DatasetMetadata firstAssignment1 =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName1).build());

    Metadata.DatasetMetadata firstAssignment2 =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName2).build());

    assertThat(firstAssignment1.getThroughputBytes()).isEqualTo(12000000);
    assertThat(firstAssignment1.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(firstAssignment1.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2", "3"));
    assertThat(firstAssignment1.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(firstAssignment1.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(firstAssignment2.getThroughputBytes()).isEqualTo(2000000);
    assertThat(firstAssignment2.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(firstAssignment2.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(firstAssignment2.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(firstAssignment2.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 5000000));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 5000000));
    assertThat(partitionMetadataStore.getSync("3"))
        .isEqualTo(createSharedPartitionMetadata("3", 4000000));

    DatasetMetadata firstDatasetMetadata1 = datasetMetadataStore.getSync(datasetName1);
    assertThat(firstDatasetMetadata1.getName()).isEqualTo(datasetName1);
    assertThat(firstDatasetMetadata1.getOwner()).isEqualTo(datasetOwner1);
    assertThat(firstDatasetMetadata1.getThroughputBytes()).isEqualTo(12000000);
    assertThat(firstDatasetMetadata1.getPartitionConfigs().size()).isEqualTo(1);

    DatasetMetadata firstDatasetMetadata2 = datasetMetadataStore.getSync(datasetName2);
    assertThat(firstDatasetMetadata2.getName()).isEqualTo(datasetName2);
    assertThat(firstDatasetMetadata2.getOwner()).isEqualTo(datasetOwner2);
    assertThat(firstDatasetMetadata2.getThroughputBytes()).isEqualTo(2000000);
    assertThat(firstDatasetMetadata2.getPartitionConfigs().size()).isEqualTo(1);

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

    Metadata.DatasetMetadata secondAssignment1 =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName1).build());

    Metadata.DatasetMetadata secondAssignment2 =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName2).build());

    assertThat(secondAssignment1.getThroughputBytes()).isEqualTo(12000000);
    assertThat(secondAssignment1.getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(secondAssignment1.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2", "3"));
    assertThat(secondAssignment1.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(secondAssignment1.getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(secondAssignment1.getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(secondAssignment1.getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(secondAssignment2.getThroughputBytes()).isEqualTo(2000000);
    assertThat(secondAssignment2.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(secondAssignment2.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(secondAssignment2.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(secondAssignment2.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 1000000));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 1000000));
    assertThat(partitionMetadataStore.getSync("3"))
        .isEqualTo(createDedicatedPartitionMetadata("3", 4000000));
    assertThat(partitionMetadataStore.getSync("4"))
        .isEqualTo(createDedicatedPartitionMetadata("4", 4000000));
    assertThat(partitionMetadataStore.getSync("5"))
        .isEqualTo(createDedicatedPartitionMetadata("5", 4000000));

    DatasetMetadata secondDatasetMetadata1 = datasetMetadataStore.getSync(datasetName1);
    assertThat(secondDatasetMetadata1.getName()).isEqualTo(datasetName1);
    assertThat(secondDatasetMetadata1.getOwner()).isEqualTo(datasetOwner1);
    assertThat(secondDatasetMetadata1.getThroughputBytes()).isEqualTo(12000000);
    assertThat(secondDatasetMetadata1.getPartitionConfigs().size()).isEqualTo(2);

    DatasetMetadata secondDatasetMetadata2 = datasetMetadataStore.getSync(datasetName2);
    assertThat(secondDatasetMetadata2.getName()).isEqualTo(datasetName2);
    assertThat(secondDatasetMetadata2.getOwner()).isEqualTo(datasetOwner2);
    assertThat(secondDatasetMetadata2.getThroughputBytes()).isEqualTo(2000000);
    assertThat(secondDatasetMetadata2.getPartitionConfigs().size()).isEqualTo(1);

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

    Metadata.DatasetMetadata thirdAssignment1 =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName1).build());

    Metadata.DatasetMetadata thirdAssignment2 =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName2).build());

    assertThat(thirdAssignment1.getThroughputBytes()).isEqualTo(8000000);
    assertThat(thirdAssignment1.getPartitionConfigsList().size()).isEqualTo(3);
    assertThat(thirdAssignment1.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2", "3"));
    assertThat(thirdAssignment1.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);
    assertThat(thirdAssignment1.getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(thirdAssignment1.getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(thirdAssignment1.getPartitionConfigsList().get(2).getPartitionsList())
        .isEqualTo(List.of("3", "4"));
    assertThat(thirdAssignment1.getPartitionConfigsList().get(2).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(thirdAssignment1.getPartitionConfigsList().get(2).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(thirdAssignment2.getThroughputBytes()).isEqualTo(2000000);
    assertThat(thirdAssignment2.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(thirdAssignment2.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(thirdAssignment2.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(thirdAssignment2.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 1000000));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 1000000));
    assertThat(partitionMetadataStore.getSync("3"))
        .isEqualTo(createSharedPartitionMetadata("3", 4000000));
    assertThat(partitionMetadataStore.getSync("4"))
        .isEqualTo(createSharedPartitionMetadata("4", 4000000));
    assertThat(partitionMetadataStore.getSync("5"))
        .isEqualTo(createSharedPartitionMetadata("5", 0));

    DatasetMetadata thirdDatasetMetadata1 = datasetMetadataStore.getSync(datasetName1);
    assertThat(thirdDatasetMetadata1.getName()).isEqualTo(datasetName1);
    assertThat(thirdDatasetMetadata1.getOwner()).isEqualTo(datasetOwner1);
    assertThat(thirdDatasetMetadata1.getThroughputBytes()).isEqualTo(8000000);
    assertThat(thirdDatasetMetadata1.getPartitionConfigs().size()).isEqualTo(3);

    DatasetMetadata thirdDatasetMetadata2 = datasetMetadataStore.getSync(datasetName2);
    assertThat(thirdDatasetMetadata2.getName()).isEqualTo(datasetName2);
    assertThat(thirdDatasetMetadata2.getOwner()).isEqualTo(datasetOwner2);
    assertThat(thirdDatasetMetadata2.getThroughputBytes()).isEqualTo(2000000);
    assertThat(thirdDatasetMetadata2.getPartitionConfigs().size()).isEqualTo(1);

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

    Metadata.DatasetMetadata fourthAssignment1 =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName1).build());

    Metadata.DatasetMetadata fourthAssignment2 =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName2).build());

    assertThat(fourthAssignment1.getThroughputBytes()).isEqualTo(12000000);
    assertThat(fourthAssignment1.getPartitionConfigsList().size()).isEqualTo(4);
    assertThat(fourthAssignment1.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2", "3"));
    assertThat(fourthAssignment1.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);
    assertThat(fourthAssignment1.getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(fourthAssignment1.getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);
    assertThat(fourthAssignment1.getPartitionConfigsList().get(2).getPartitionsList())
        .isEqualTo(List.of("3", "4"));
    assertThat(fourthAssignment1.getPartitionConfigsList().get(2).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);
    assertThat(fourthAssignment1.getPartitionConfigsList().get(3).getPartitionsList())
        .isEqualTo(List.of("3", "4", "1"));
    assertThat(fourthAssignment1.getPartitionConfigsList().get(3).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(fourthAssignment1.getPartitionConfigsList().get(3).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(fourthAssignment2.getThroughputBytes()).isEqualTo(2000000);
    assertThat(fourthAssignment2.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(fourthAssignment2.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(fourthAssignment2.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(fourthAssignment2.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    assertThat(partitionMetadataStore.getSync("1"))
        .isEqualTo(createSharedPartitionMetadata("1", 5000000));
    assertThat(partitionMetadataStore.getSync("2"))
        .isEqualTo(createSharedPartitionMetadata("2", 1000000));
    assertThat(partitionMetadataStore.getSync("3"))
        .isEqualTo(createSharedPartitionMetadata("3", 4000000));
    assertThat(partitionMetadataStore.getSync("4"))
        .isEqualTo(createSharedPartitionMetadata("4", 4000000));
    assertThat(partitionMetadataStore.getSync("5"))
        .isEqualTo(createSharedPartitionMetadata("5", 0));

    DatasetMetadata fourthDatasetMetadata1 = datasetMetadataStore.getSync(datasetName1);
    assertThat(fourthDatasetMetadata1.getName()).isEqualTo(datasetName1);
    assertThat(fourthDatasetMetadata1.getOwner()).isEqualTo(datasetOwner1);
    assertThat(fourthDatasetMetadata1.getThroughputBytes()).isEqualTo(12000000);
    assertThat(fourthDatasetMetadata1.getPartitionConfigs().size()).isEqualTo(4);

    DatasetMetadata fourthDatasetMetadata2 = datasetMetadataStore.getSync(datasetName2);
    assertThat(fourthDatasetMetadata2.getName()).isEqualTo(datasetName2);
    assertThat(fourthDatasetMetadata2.getOwner()).isEqualTo(datasetOwner2);
    assertThat(fourthDatasetMetadata2.getThroughputBytes()).isEqualTo(2000000);
    assertThat(fourthDatasetMetadata2.getPartitionConfigs().size()).isEqualTo(1);
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
    return new PartitionMetadata(partitionNumber, 0, 5000000, false);
  }

  @Test
  public void shouldErrorUpdatingPartitionAutoAssignmentNotEnoughPartitions() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    partitionMetadataStore.createSync(createSharedPartitionMetadata("1"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("2"));

    Metadata.DatasetMetadata initialDatasetRequest =
        managerApiStub.createDatasetMetadata(
            ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(datasetOwner)
                .build());
    assertThat(initialDatasetRequest.getPartitionConfigsList().size()).isEqualTo(0);

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(15000000)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getMessage())
        .isEqualTo(
            "UNKNOWN: Error updating partition assignment, could not find partitions to assign");

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(1);

    Metadata.DatasetMetadata firstAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());
    assertThat(firstAssignment.getPartitionConfigsList().size()).isEqualTo(0);

    PartitionMetadata partitionMetadata1 = partitionMetadataStore.getSync("1");
    assertThat(partitionMetadata1.getProvisionedCapacity()).isEqualTo(0);
    assertThat(partitionMetadata1.getDedicatedPartition()).isEqualTo(false);
    PartitionMetadata partitionMetadata2 = partitionMetadataStore.getSync("2");
    assertThat(partitionMetadata2.getProvisionedCapacity()).isEqualTo(0);
    assertThat(partitionMetadata2.getDedicatedPartition()).isEqualTo(false);
  }

  @Test
  public void shouldErrorUpdatingPartitionAssignmentNonexistentDataset() {
    String datasetName = "testDataset";
    List<String> partitionList = List.of("1", "2");

    partitionMetadataStore.createSync(createSharedPartitionMetadata("1"));
    partitionMetadataStore.createSync(createSharedPartitionMetadata("2"));

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(-1)
                            .addAllPartitionIds(partitionList)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

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
