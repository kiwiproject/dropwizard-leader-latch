package org.kiwiproject.curator.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.kiwiproject.curator.leader.util.CuratorTestHelpers.closeIfStarted;
import static org.kiwiproject.curator.leader.util.CuratorTestHelpers.deleteRecursivelyIfExists;
import static org.kiwiproject.curator.leader.util.CuratorTestHelpers.startAndAwait;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.RetryOneTime;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.kiwiproject.curator.leader.exception.ManagedLeaderLatchException;
import org.kiwiproject.test.curator.CuratorTestingServerExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@DisplayName("ManagedLeaderLatch")
@ExtendWith(SoftAssertionsExtension.class)
@Slf4j
class ManagedLeaderLatchTest {

    @RegisterExtension
    static final CuratorTestingServerExtension ZK_TEST_SERVER = new CuratorTestingServerExtension();

    private CuratorFramework client;
    private ManagedLeaderLatch leaderLatch1;
    private LeaderLatchListener leaderListener1;
    private ManagedLeaderLatch leaderLatch2;
    private LeaderLatchListener leaderListener2;

    @BeforeEach
    void setUp() throws Exception {
        client = ZK_TEST_SERVER.getClient();

        leaderListener1 = mock(LeaderLatchListener.class);
        leaderLatch1 = new ManagedLeaderLatch(client, "id-12345", "test-service", leaderListener1);

        leaderListener2 = mock(LeaderLatchListener.class);
        leaderLatch2 = new ManagedLeaderLatch(client, "id-67890", "test-service", leaderListener2);

        if (exists(leaderLatch1.getLatchPath())) {
            deleteRecursivelyIfExists(client, leaderLatch1.getLatchPath());
        }
    }

    @AfterEach
    void tearDown() {
        closeIfStarted(leaderLatch1);
        closeIfStarted(leaderLatch2);
    }

    @ParameterizedTest
    @CsvSource({
            "test-service, 42.0.0, host1, 8080, test-service/42.0.0/host1:8080",
            "test-service, 42.0.0, host1, 9090, test-service/42.0.0/host1:9090",
            "test-service, 84.0.0, host4, 8080, test-service/84.0.0/host4:8080",
            "other-service, 21.0.42, host9, 9100, other-service/21.0.42/host9:9100",
    })
    void shouldCreateLeaderLatchId(String serviceName,
                                   String serviceVersion,
                                   String hostname,
                                   int port,
                                   String expectedId) {

        var serviceDescriptor = ServiceDescriptor.builder()
                .name(serviceName)
                .version(serviceVersion)
                .hostname(hostname)
                .port(port)
                .build();

        assertThat(ManagedLeaderLatch.leaderLatchId(serviceDescriptor)).isEqualTo(expectedId);
    }

    @ParameterizedTest
    @CsvSource({
            "a-service, /kiwi/leader-latch/a-service/leader-latch",
            "other-service, /kiwi/leader-latch/other-service/leader-latch",
            "one-more-service, /kiwi/leader-latch/one-more-service/leader-latch",
    })
    void shouldCreateLeaderLatchPath(String serviceName, String expectedPath) {
        assertThat(ManagedLeaderLatch.leaderLatchPath(serviceName)).isEqualTo(expectedPath);
    }

    @Test
    void shouldConstructNewLatch(SoftAssertions softly) {
        var latch = new ManagedLeaderLatch(client, "id-12345", "test-service", leaderListener1);

        softly.assertThat(latch.getId()).isEqualTo("id-12345");
        softly.assertThat(latch.getLatchPath()).isEqualTo("/kiwi/leader-latch/test-service/leader-latch");
        softly.assertThat(latch.getLatchState()).isEqualTo(LeaderLatch.State.LATENT);
    }

    @Test
    void shouldConstructNewLeaderLatch_UsingServiceDescriptor(SoftAssertions softly) {
        var service = ServiceDescriptor.builder()
                .name("test-service")
                .version("42.0.24")
                .hostname("host-12345")
                .port(8901)
                .build();

        var latch = new ManagedLeaderLatch(client, service, leaderListener1);

        softly.assertThat(latch.getId()).isEqualTo("test-service/42.0.24/host-12345:8901");
        softly.assertThat(latch.getLatchPath()).isEqualTo("/kiwi/leader-latch/test-service/leader-latch");
        softly.assertThat(latch.getLatchState()).isEqualTo(LeaderLatch.State.LATENT);
    }

    @Test
    void shouldCreateNewLatchNode_WhenDoesNotExist() throws Exception {
        assertThat(exists(leaderLatch1.getLatchPath())).isFalse();
        startAndAwait(leaderLatch1);

        assertThat(exists(leaderLatch1.getLatchPath())).isTrue();
        assertThat(leaderLatch1.getParticipants()).hasSize(1);
    }

    @Test
    void shouldIgnore_WhenStartCalledMoreThanOnce() throws Exception {
        assertThat(exists(leaderLatch1.getLatchPath())).isFalse();
        startAndAwait(leaderLatch1);

        leaderLatch1.start();
        leaderLatch1.start();

        assertThat(exists(leaderLatch1.getLatchPath())).isTrue();
        assertThat(leaderLatch1.getParticipants()).hasSize(1);
    }

    private boolean exists(String path) throws Exception {
        return client.checkExists().forPath(path) != null;
    }

    @Test
    void shouldChangeState() throws Exception {
        assertThat(leaderLatch1.getLatchState()).isEqualTo(LeaderLatch.State.LATENT);

        startAndAwait(leaderLatch1);
        assertThat(leaderLatch1.getLatchState()).isEqualTo(LeaderLatch.State.STARTED);

        closeIfStarted(leaderLatch1);
        assertThat(leaderLatch1.getLatchState()).isEqualTo(LeaderLatch.State.CLOSED);
    }

    @Test
    void shouldReportLeadership_WhenMultipleLatchParticipants() throws Exception {
        startAndAwait(leaderLatch1, leaderLatch2);

        var nodes = client.getChildren().forPath(leaderLatch1.getLatchPath());
        assertThat(nodes).hasSize(2);
        assertThat(atLeastOneHasLeadership()).isTrue();
        assertThat(bothAreLeaders()).isFalse();
    }

    private boolean atLeastOneHasLeadership() {
        return leaderLatch1.hasLeadership() || leaderLatch2.hasLeadership();
    }

    private boolean bothAreLeaders() {
        return leaderLatch1.hasLeadership() && leaderLatch2.hasLeadership();
    }

    @Test
    void shouldThrowException_WhenHasLeadershipCalled_WhenCuratorIsNotStarted() {
        var retryPolicy = new RetryOneTime(500);
        var curatorClient = CuratorFrameworkFactory.newClient(ZK_TEST_SERVER.getConnectString(), retryPolicy);
        var latch = new ManagedLeaderLatch(curatorClient, "testLatchId", "test-service");
        LOG.trace("Created latch {}", latch);

        assertThatThrownBy(latch::hasLeadership)
                .isExactlyInstanceOf(ManagedLeaderLatchException.class)
                .hasMessage("Curator must be started before calling this method. Curator state: " +
                        CuratorFrameworkState.LATENT.name());
    }

    @Test
    void shouldThrowException_WhenHasLeadershipCalled_WhenLeaderLatchIsNotStarted() {
        var retryPolicy = new RetryOneTime(500);
        var curatorClient = CuratorFrameworkFactory.newClient(ZK_TEST_SERVER.getConnectString(), retryPolicy);
        var latch = new ManagedLeaderLatch(curatorClient, "testLatchId", "test-service");
        LOG.trace("Created latch {}", latch);

        try (curatorClient) {
            curatorClient.start();
            assertThatThrownBy(latch::hasLeadership)
                    .isExactlyInstanceOf(ManagedLeaderLatchException.class)
                    .hasMessage("LeaderLatch must be started before calling this method. Latch state: " +
                            LeaderLatch.State.LATENT.name());
        }
    }

    @Test
    void shouldThrowException_WhenHasLeadershipCalled_WhenLeaderLatchHasNoParticipantsYet() throws Exception {
        var retryPolicy = new RetryOneTime(500);
        var curatorClient = CuratorFrameworkFactory.newClient(ZK_TEST_SERVER.getConnectString(), retryPolicy);
        var latch = new ManagedLeaderLatch(curatorClient, "testLatchId", "test-vip-address") {
            @Override
            public Collection<Participant> getParticipants() {
                return Collections.emptyList();
            }
        };
        LOG.trace("Created latch {} that always returns empty participants", latch);

        try (curatorClient) {
            curatorClient.start();
            latch.start();

            assertThatThrownBy(latch::hasLeadership)
                    .isExactlyInstanceOf(ManagedLeaderLatchException.class)
                    .hasMessage("LeaderLatch must have participants before calling this method.");
        } finally {
            latch.stop();
        }
    }

    @Test
    void shouldChangeLeadership() throws Exception {
        startAndAwait(leaderLatch1);
        assertThat(leaderLatch1.hasLeadership()).isTrue();

        startAndAwait(leaderLatch2);
        assertThat(leaderLatch2.hasLeadership()).isFalse();

        closeIfStarted(leaderLatch1);

        assertThat(leaderLatch2.hasLeadership()).isTrue();
    }

    @Test
    void shouldNotify_LeaderChangeNotifications_InOrder() throws Exception {
        startAndAwait(leaderLatch1);
        startAndAwait(leaderLatch2);
        closeIfStarted(leaderLatch1);
        closeIfStarted(leaderLatch2);

        var inOrder = inOrder(leaderListener1, leaderListener2);
        inOrder.verify(leaderListener1).isLeader();
        inOrder.verify(leaderListener1).notLeader();
        inOrder.verify(leaderListener2).isLeader();
        inOrder.verify(leaderListener2).notLeader();
    }

    @Test
    void shouldNotifyAllListeners_WhenThereAreMultipleListeners() throws Exception {
        var leaderLatchListener1a = mock(LeaderLatchListener.class);
        var leaderLatchListener1b = mock(LeaderLatchListener.class);
        var latchWithListeners = new ManagedLeaderLatch(
                client, "id-12345", "test-service", leaderLatchListener1a, leaderLatchListener1b);

        startAndAwait(latchWithListeners);
        closeIfStarted(latchWithListeners);

        var inOrder1 = inOrder(leaderLatchListener1a);
        inOrder1.verify(leaderLatchListener1a).isLeader();
        inOrder1.verify(leaderLatchListener1a).notLeader();

        var inOrder2 = inOrder(leaderLatchListener1b);
        inOrder2.verify(leaderLatchListener1b).isLeader();
        inOrder2.verify(leaderLatchListener1b).notLeader();
    }

    @Test
    void shouldStartAndStop_WhenNoListeners() throws Exception {
        var leaderLatch = new ManagedLeaderLatch(client, "id-4242", "test-service");
        startAndAwait(leaderLatch);

        try {
            assertThat(leaderLatch.getLatchState()).isEqualTo(LeaderLatch.State.STARTED);
        } finally {
            closeIfStarted(leaderLatch);
        }

        assertThat(leaderLatch.getLatchState()).isEqualTo(LeaderLatch.State.CLOSED);
    }

    @Test
    void shouldGetParticipants() throws Exception {
        startAndAwait(leaderLatch1);

        assertThat(leaderLatch1.getParticipants())
                .containsExactly(new Participant(leaderLatch1.getId(), true));

        startAndAwait(leaderLatch2);

        var expectedParticipants = List.of(
                new Participant(leaderLatch1.getId(), true),
                new Participant(leaderLatch2.getId(), false)
        );

        assertThat(leaderLatch1.getParticipants())
                .hasSize(2)
                .containsAll(expectedParticipants);

        assertThat(leaderLatch2.getParticipants())
                .hasSize(2)
                .containsAll(expectedParticipants);
    }

    @Test
    void shouldGetLeader() throws Exception {
        startAndAwait(leaderLatch1);

        assertThat(leaderLatch1.getLeader()).isEqualTo(new Participant(leaderLatch1.getId(), true));

        startAndAwait(leaderLatch2);

        assertThat(leaderLatch1.getLeader())
                .describedAs("Both latch instances should have same leader")
                .isEqualTo(leaderLatch2.getLeader());

        assertThat(leaderLatch1.getLeader())
                .describedAs("leaderLatch1 is still leader")
                .isEqualTo(new Participant(leaderLatch1.getId(), true));

        closeIfStarted(leaderLatch1);

        assertThat(leaderLatch2.getLeader())
                .describedAs("leaderLatch2 should now be leader")
                .isEqualTo(new Participant(leaderLatch2.getId(), true));
    }

    @Test
    void shouldCallActionSynchronously_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);

        var called = new AtomicBoolean();
        leaderLatch1.whenLeader(() -> called.set(true));

        assertThat(called).isTrue();
    }

    @Test
    void shouldNotCallActionSynchronously_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        startAndAwait(leaderLatch2);
        assertThat(leaderLatch1.hasLeadership()).isTrue();

        var called = new AtomicBoolean();
        leaderLatch2.whenLeader(() -> called.set(true));

        assertThat(called).isFalse();
    }

    @Test
    void shouldCallActionAsynchronously_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);
        assertThat(leaderLatch1.hasLeadership()).isTrue();

        var called = new AtomicBoolean();
        Optional<CompletableFuture<Void>> result = leaderLatch1.whenLeaderAsync(() -> called.set(true));

        assertThat(result).isPresent();
        CompletableFuture<Void> future = result.orElseThrow();
        await().atMost(FIVE_SECONDS).until(future::isDone);
        assertThat(called).isTrue();
    }

    @Test
    void shouldNotCallActionAsynchronously_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        startAndAwait(leaderLatch2);
        assertThat(leaderLatch1.hasLeadership()).isTrue();

        var called = new AtomicBoolean();
        Optional<CompletableFuture<Void>> result = leaderLatch2.whenLeaderAsync(() -> called.set(true));

        assertThat(result).isEmpty();
        assertThat(called).isFalse();
    }

    @Test
    void shouldCallSupplierSynchronously_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);
        Supplier<Integer> supplier = () -> 42;

        Optional<Integer> result = leaderLatch1.whenLeader(supplier);
        assertThat(result).contains(42);
    }

    @Test
    void shouldNotCallSupplierSynchronously_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        startAndAwait(leaderLatch2);
        assertThat(leaderLatch1.hasLeadership()).isTrue();

        Optional<Integer> result = leaderLatch2.whenLeader(() -> 42);
        assertThat(result).isEmpty();
    }

    @Test
    void shouldCallSupplierAsynchronously_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);
        Supplier<Integer> supplier = () -> 42;

        Optional<CompletableFuture<Integer>> result = leaderLatch1.whenLeaderAsync(supplier);
        assertThat(result).isPresent();

        CompletableFuture<Integer> future = result.orElseThrow();
        await().atMost(FIVE_SECONDS).until(future::isDone);
        assertThat(future.get()).isEqualTo(42);
    }

    @Test
    void shouldNotCallSupplierAsynchronously_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        startAndAwait(leaderLatch2);
        assertThat(leaderLatch1.hasLeadership()).isTrue();

        Optional<CompletableFuture<Integer>> result = leaderLatch2.whenLeaderAsync(() -> 42);
        assertThat(result).isEmpty();
    }

    @Test
    void shouldCallSupplierAsynchronously_WithCustomExecutor_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);
        Supplier<Integer> supplier = () -> 42;
        var executor = Executors.newSingleThreadExecutor();

        Optional<CompletableFuture<Integer>> result = leaderLatch1.whenLeaderAsync(supplier, executor);

        assertThat(result).isNotEmpty();
        CompletableFuture<Integer> future = result.orElseThrow();
        await().atMost(FIVE_SECONDS).until(future::isDone);
        assertThat(future.get()).isEqualTo(42);
    }

    @Test
    void shouldNotCallSupplierAsynchronously_WithCustomExecutor_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        startAndAwait(leaderLatch2);
        assertThat(leaderLatch1.hasLeadership()).isTrue();

        var nullExecutor = nullExecutorWhichShouldNeverBeUsed();
        Optional<CompletableFuture<Integer>> result = leaderLatch2.whenLeaderAsync(() -> 42, nullExecutor);

        assertThat(result).isEmpty();
    }

    private Executor nullExecutorWhichShouldNeverBeUsed() {
        return null;
    }
}
