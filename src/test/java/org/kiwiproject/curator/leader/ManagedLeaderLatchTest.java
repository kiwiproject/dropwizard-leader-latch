package org.kiwiproject.curator.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.curator.leader.util.AwaitilityTestHelpers.await5SecondsUntilAsserted;
import static org.kiwiproject.curator.leader.util.AwaitilityTestHelpers.await5SecondsUntilFutureIsDone;
import static org.kiwiproject.curator.leader.util.AwaitilityTestHelpers.await5SecondsUntilTrue;
import static org.kiwiproject.curator.leader.util.CuratorTestHelpers.closeIfStarted;
import static org.kiwiproject.curator.leader.util.CuratorTestHelpers.deleteRecursivelyIfExists;
import static org.kiwiproject.curator.leader.util.CuratorTestHelpers.startAndAwait;
import static org.kiwiproject.test.assertj.KiwiAssertJ.assertIsExactType;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

import com.google.common.base.VerifyException;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.KeeperException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.kiwiproject.curator.leader.LeadershipStatus.CuratorNotStarted;
import org.kiwiproject.curator.leader.LeadershipStatus.IsLeader;
import org.kiwiproject.curator.leader.LeadershipStatus.LatchNotStarted;
import org.kiwiproject.curator.leader.LeadershipStatus.NoLatchParticipants;
import org.kiwiproject.curator.leader.LeadershipStatus.NotLeader;
import org.kiwiproject.curator.leader.LeadershipStatus.OtherError;
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
    void shouldConstructNewLatch() {
        var latch = new ManagedLeaderLatch(client, "id-12345", "test-service", leaderListener1);

        assertAll(
                () -> assertThat(latch.getId()).isEqualTo("id-12345"),
                () -> assertThat(latch.getLatchPath()).isEqualTo("/kiwi/leader-latch/test-service/leader-latch"),
                () -> assertThat(latch.getLatchState()).isEqualTo(LeaderLatch.State.LATENT)
        );
    }

    @Test
    void shouldConstructNewLeaderLatch_UsingServiceDescriptor() {
        var service = ServiceDescriptor.builder()
                .name("test-service")
                .version("42.0.24")
                .hostname("host-12345")
                .port(8901)
                .build();

        var latch = new ManagedLeaderLatch(client, service, leaderListener1);

        assertAll(
                () -> assertThat(latch.getId()).isEqualTo("test-service/42.0.24/host-12345:8901"),
                () -> assertThat(latch.getLatchPath()).isEqualTo("/kiwi/leader-latch/test-service/leader-latch"),
                () -> assertThat(latch.getLatchState()).isEqualTo(LeaderLatch.State.LATENT)
        );
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

        await5SecondsUntilAsserted(() -> {
            assertThat(atLeastOneHasLeadership()).isTrue();
            assertThat(bothAreLeaders()).isFalse();
        });
    }

    private boolean atLeastOneHasLeadership() {
        return leaderLatch1.hasLeadership() || leaderLatch2.hasLeadership();
    }

    private boolean bothAreLeaders() {
        return leaderLatch1.hasLeadership() && leaderLatch2.hasLeadership();
    }

    @ParameterizedTest
    @EnumSource(value = CuratorFrameworkState.class, names = {"STARTED"}, mode = EnumSource.Mode.EXCLUDE)
    void shouldThrowException_WhenStartCalled_ButCuratorIsNotStarted(CuratorFrameworkState curatorState) {
        var notStartedClient = mock(CuratorFramework.class);
        when(notStartedClient.getState()).thenReturn(curatorState);

        var latch = new ManagedLeaderLatch(notStartedClient, "id-12345", "test-service", leaderListener1);

        assertThatExceptionOfType(VerifyException.class)
                .isThrownBy(latch::start)
                .withMessage("CuratorFramework must be started");
    }

    @Test
    void shouldThrowException_WhenStartCalled_ButExceptionThrownCreatingNode() {
        var spyClient = spy(client);
        var exception = new IllegalStateException("Curator is stopped!");
        doThrow(exception).when(spyClient).create();

        var latch = new ManagedLeaderLatch(spyClient, "id-1234", "test-service");

        assertThatExceptionOfType(ManagedLeaderLatchException.class)
                .isThrownBy(latch::start)
                .havingCause()
                .isSameAs(exception);
    }

    @Test
    void shouldThrowException_WhenHasLeadershipCalled_WhenCuratorIsNotStarted() {
        var latch = setupLatch().latch();

        assertThatThrownBy(latch::hasLeadership)
                .isExactlyInstanceOf(ManagedLeaderLatchException.class)
                .hasMessage("Curator must be started and not closed before calling this method. Curator state: " +
                        CuratorFrameworkState.LATENT.name());
    }

    @Test
    void shouldThrowException_WhenHasLeadershipCalled_WhenLeaderLatchIsNotStarted() {
        var testContext = setupLatch();

        try (var curatorFramework = testContext.curatorClient()) {
            curatorFramework.start();
            assertThatThrownBy(testContext.latch()::hasLeadership)
                    .isExactlyInstanceOf(ManagedLeaderLatchException.class)
                    .hasMessage("LeaderLatch must be started and not closed before calling this method. Latch state: " +
                            LeaderLatch.State.LATENT.name());
        }
    }

    @Test
    void shouldThrowException_WhenHasLeadershipCalled_WhenLeaderLatchHasNoParticipantsYet() throws Exception {
        var testContext = setupLatchWithNoParticipants();
        var latch = testContext.latch();

        try (var curatorClient = testContext.curatorClient()) {
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
        awaitIsLeader(leaderLatch1);

        startAndAwait(leaderLatch2);
        awaitNotLeader(leaderLatch2);

        closeIfStarted(leaderLatch1);

        awaitIsLeader(leaderLatch2);
    }

    @Test
    void shouldReportOppositeValueFor_hasLeadership_and_doesNotHaveLeadership() throws Exception {
        startAndAwait(leaderLatch1);
        startAndAwait(leaderLatch2);

        awaitIsLeader(leaderLatch1);
        awaitNotLeader(leaderLatch2);

        closeIfStarted(leaderLatch1);  // once closed, we cannot call hasLeadership or doesNotHaveLeadership

        awaitIsLeader(leaderLatch2);
    }

    @Test
    void shouldNotify_LeaderChangeNotifications_InOrder() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        startAndAwait(leaderLatch2);

        closeIfStarted(leaderLatch1);
        awaitIsLeader(leaderLatch2);

        closeIfStarted(leaderLatch2);

        // Listener callbacks are async; use Mockito timeout to avoid race with Curator event thread
        var inOrder = inOrder(leaderListener1, leaderListener2);
        inOrder.verify(leaderListener1, timeout(5000)).isLeader();
        inOrder.verify(leaderListener1, timeout(5000)).notLeader();
        inOrder.verify(leaderListener2, timeout(5000)).isLeader();
        inOrder.verify(leaderListener2, timeout(5000)).notLeader();
    }

    @Test
    void shouldNotifyAllListeners_WhenThereAreMultipleListeners() throws Exception {
        var leaderLatchListener1a = mock(LeaderLatchListener.class);
        var leaderLatchListener1b = mock(LeaderLatchListener.class);
        var latchWithListeners = new ManagedLeaderLatch(
                client, "id-12345", "test-service", leaderLatchListener1a, leaderLatchListener1b);

        var leader1aCalled = new AtomicBoolean();
        doAnswer(invocation -> {
            leader1aCalled.set(true);
            return null;
        }).when(leaderLatchListener1a).isLeader();

        var leader1bCalled = new AtomicBoolean();
        doAnswer(invocation -> {
            leader1bCalled.set(true);
            return null;
        }).when(leaderLatchListener1b).isLeader();

        try {
            startAndAwait(latchWithListeners);

            await5SecondsUntilTrue(leader1aCalled);
            await5SecondsUntilTrue(leader1bCalled);
        } finally {
            closeIfStarted(latchWithListeners);
        }

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
        awaitIsLeader(leaderLatch1);

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
    void shouldThrowException_WhenErrorOccursGettingParticipants() throws Exception {
        var leaderLatch = mock(LeaderLatch.class);
        var noNodeException = new KeeperException.NoNodeException("/latch/path");
        when(leaderLatch.getParticipants()).thenThrow(noNodeException);

        var managedLeaderLatch = new ManagedLeaderLatch(client,
                "id-2442",
                new ManagedLeaderLatch.LatchAndPath(leaderLatch, "/latch/path"));

        assertThatExceptionOfType(ManagedLeaderLatchException.class)
                .isThrownBy(managedLeaderLatch::getParticipants)
                .havingCause()
                .isSameAs(noNodeException);
    }

    @Test
    void shouldGetLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        assertThat(leaderLatch1.getLeader()).isEqualTo(new Participant(leaderLatch1.getId(), true));

        startAndAwait(leaderLatch2);

        assertThat(leaderLatch1.getLeader())
                .describedAs("Both latch instances should have same leader")
                .isEqualTo(leaderLatch2.getLeader());

        assertThat(leaderLatch1.getLeader())
                .describedAs("leaderLatch1 is still leader")
                .isEqualTo(new Participant(leaderLatch1.getId(), true));

        closeIfStarted(leaderLatch1);

        await5SecondsUntilAsserted(() ->
                assertThat(leaderLatch2.getLeader())
                        .describedAs("leaderLatch2 should now be leader")
                        .isEqualTo(new Participant(leaderLatch2.getId(), true))
        );
    }

    @Test
    void shouldThrowException_WhenAnErrorOccurs_GettingTheLeader() throws Exception {
        var leaderLatch = mock(LeaderLatch.class);
        var noNodeException = new KeeperException.NoNodeException("/latch/path");
        when(leaderLatch.getLeader()).thenThrow(noNodeException);

        var managedLeaderLatch = new ManagedLeaderLatch(client,
                "id-2442",
                new ManagedLeaderLatch.LatchAndPath(leaderLatch, "/latch/path"));

        assertThatExceptionOfType(ManagedLeaderLatchException.class)
                .isThrownBy(managedLeaderLatch::getLeader)
                .havingCause()
                .isSameAs(noNodeException);
    }

    @Test
    void shouldCheck_IsClosed() throws Exception {
        startAndAwait(leaderLatch1);
        assertThat(leaderLatch1.isClosed()).isFalse();

        leaderLatch1.stop();
        assertThat(leaderLatch1.isClosed()).isTrue();
    }

    @Test
    void shouldCallActionSynchronously_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);

        var called = new AtomicBoolean();
        leaderLatch1.whenLeader(() -> called.set(true));

        await5SecondsUntilTrue(called);
    }

    // Ignore Sonar's warning about the sleep call here. This test asserts that an action is NOT invoked.
    // There is no event to await, and a short, bounded sleep is the safest way to detect unintended
    // asynchronous execution.
    @Test
    @SuppressWarnings("java:S2925")
    void shouldNotCallActionSynchronously_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        startAndAwait(leaderLatch2);

        var called = new AtomicBoolean();
        leaderLatch2.whenLeader(() -> called.set(true));

        Thread.sleep(200);
        assertThat(called).isFalse();
    }

    @Test
    void shouldCallActionAsynchronously_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        var called = new AtomicBoolean();
        Optional<CompletableFuture<Void>> result = leaderLatch1.whenLeaderAsync(() -> called.set(true));

        assertThat(result).isPresent();
        CompletableFuture<Void> future = result.orElseThrow();
        await5SecondsUntilFutureIsDone(future);
        assertThat(called).isTrue();
    }

    @Test
    void shouldNotCallActionAsynchronously_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        startAndAwait(leaderLatch2);

        var called = new AtomicBoolean();
        Optional<CompletableFuture<Void>> result = leaderLatch2.whenLeaderAsync(() -> called.set(true));

        assertThat(result).isEmpty();
        assertThat(called).isFalse();
    }

    @Test
    void shouldCallSupplierSynchronously_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        Supplier<Integer> supplier = () -> 42;

        Optional<Integer> result = leaderLatch1.whenLeader(supplier);
        assertThat(result).contains(42);
    }

    @Test
    void shouldNotCallSupplierSynchronously_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        startAndAwait(leaderLatch2);

        Optional<Integer> result = leaderLatch2.whenLeader(() -> 42);
        assertThat(result).isEmpty();
    }

    @Test
    void shouldCallSupplierAsynchronously_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        Supplier<Integer> supplier = () -> 42;

        Optional<CompletableFuture<Integer>> result = leaderLatch1.whenLeaderAsync(supplier);
        assertThat(result).isPresent();

        CompletableFuture<Integer> future = result.orElseThrow();
        await5SecondsUntilFutureIsDone(future);
        assertThat(future).isCompletedWithValue(42);
    }

    @Test
    void shouldNotCallSupplierAsynchronously_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        startAndAwait(leaderLatch2);

        Optional<CompletableFuture<Integer>> result = leaderLatch2.whenLeaderAsync(() -> 42);
        assertThat(result).isEmpty();
    }

    @Test
    void shouldCallSupplierAsynchronously_WithCustomExecutor_WhenIsLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        Supplier<Integer> supplier = () -> 42;
        var executor = Executors.newSingleThreadExecutor();

        Optional<CompletableFuture<Integer>> result = leaderLatch1.whenLeaderAsync(supplier, executor);

        assertThat(result).isNotEmpty();
        CompletableFuture<Integer> future = result.orElseThrow();
        await5SecondsUntilFutureIsDone(future);
        assertThat(future).isCompletedWithValue(42);
    }

    @Test
    void shouldNotCallSupplierAsynchronously_WithCustomExecutor_WhenIsNotLeader() throws Exception {
        startAndAwait(leaderLatch1);
        awaitIsLeader(leaderLatch1);

        startAndAwait(leaderLatch2);

        var nullExecutor = nullExecutorWhichShouldNeverBeUsed();
        Optional<CompletableFuture<Integer>> result = leaderLatch2.whenLeaderAsync(() -> 42, nullExecutor);

        assertThat(result).isEmpty();
    }

    @SuppressWarnings("SameReturnValue")
    private Executor nullExecutorWhichShouldNeverBeUsed() {
        return null;
    }

    @Test
    void shouldGetManagedLeaderLatch() {
        var latch = leaderLatch1.getManagedLatch();
        assertThat(latch).isNotNull();
        assertThat(latch.getId()).isEqualTo(leaderLatch1.getId());
    }

    @Nested
    class HasLeadershipIgnoringErrors {

        @Test
        void shouldReturnExpectedValue_WhenValidState() throws Exception {
            startAndAwait(leaderLatch1, leaderLatch2);
            awaitIsLeader(leaderLatch1);

            assertAll(
                    () -> assertThat(leaderLatch1.hasLeadershipIgnoringErrors()).isTrue(),
                    () -> assertThat(leaderLatch2.hasLeadershipIgnoringErrors()).isFalse()
            );
        }

        @Test
        void shouldReturnFalse_WhenCuratorNotStarted() {
            var latch = setupLatch().latch();

            assertAll(
                    () -> assertThat(latch.hasLeadershipIgnoringErrors()).isFalse(),
                    () -> assertThat(latch.isStarted()).isFalse()
            );
        }

        @Test
        void shouldReturnFalse_WhenLatchNotStarted() {
            var testContext = setupLatch();

            try (var curatorClient = testContext.curatorClient()) {
                curatorClient.start();
                assertThat(testContext.latch().hasLeadershipIgnoringErrors()).isFalse();
            }
        }

        @Test
        void shouldReturnFalse_WhenLeaderLatchHasNoParticipantsYet() throws Exception {
            var testContext = setupLatchWithNoParticipants();
            var latch = testContext.latch();

            try (var curatorClient = testContext.curatorClient()) {
                curatorClient.start();
                latch.start();

                assertThat(latch.hasLeadershipIgnoringErrors()).isFalse();
            } finally {
                latch.stop();
            }
        }

        @Test
        void shouldReturnFalse_WhenExceptionThrownGettingParticipants() throws Exception {
            var testContext = setupLatchThatThrowsGettingParticipants();
            var latch = testContext.latch();

            try (var curatorClient = testContext.curatorClient()) {
                curatorClient.start();
                latch.start();

                assertThat(latch.hasLeadershipIgnoringErrors()).isFalse();
            } finally {
                latch.stop();
            }
        }
    }

    @Nested
    class CheckLeadershipStatus {

        @Test
        void shouldReturnExpectedIsOrNotLeader_WhenValidState() throws Exception {
            startAndAwait(leaderLatch1, leaderLatch2);
            awaitIsLeader(leaderLatch1);

            assertAll(
                    () -> assertThat(leaderLatch1.checkLeadershipStatus()).isExactlyInstanceOf(IsLeader.class),
                    () -> assertThat(leaderLatch2.checkLeadershipStatus()).isExactlyInstanceOf(NotLeader.class)
            );
        }

        @Test
        void shouldReturnCuratorNotStarted_WhenCuratorNotStarted() {
            var latch = setupLatch().latch();
            var status = latch.checkLeadershipStatus();

            var notStarted = assertIsExactType(status, CuratorNotStarted.class);
            assertThat(notStarted.curatorState()).isEqualTo(CuratorFrameworkState.LATENT);
        }

        @Test
        void shouldReturnLatchNotStarted_WhenLatchNotStarted() {
            var testContext = setupLatch();

            try (var curatorClient = testContext.curatorClient()) {
                curatorClient.start();
                var status = testContext.latch().checkLeadershipStatus();

                var latchNotStarted = assertIsExactType(status, LatchNotStarted.class);
                assertThat(latchNotStarted.latchState()).isEqualTo(LeaderLatch.State.LATENT);
            }
        }

        @Test
        void shouldReturnNoLatchParticipants_WhenLeaderLatchHasNoParticipantsYet() throws Exception {
            var testContext = setupLatchWithNoParticipants();
            var latch = testContext.latch();

            try (var curatorClient = testContext.curatorClient()) {
                curatorClient.start();
                latch.start();

                var status = latch.checkLeadershipStatus();
                assertIsExactType(status, NoLatchParticipants.class);
            } finally {
                latch.stop();
            }
        }

        @Test
        void shouldReturnOtherError_WhenExceptionThrownGettingParticipants() throws Exception {
            var testContext = setupLatchThatThrowsGettingParticipants();
            var latch = testContext.latch();

            try (var curatorClient = testContext.curatorClient()) {
                curatorClient.start();
                latch.start();

                var status = latch.checkLeadershipStatus();
                var otherError = assertIsExactType(status, OtherError.class);
                assertThat(otherError.error()).isSameAs(testContext.exception());
            } finally {
                latch.stop();
            }
        }
    }

    private static LatchTestContext setupLatch() {
        var curatorClient = newCuratorClient();
        var latch = new ManagedLeaderLatch(curatorClient, "testLatchId", "test-service");
        LOG.trace("Created latch {}", latch);
        return new LatchTestContext(latch, curatorClient, null);
    }

    private static LatchTestContext setupLatchWithNoParticipants() {
        var curatorClient = newCuratorClient();
        var latch = new ManagedLeaderLatch(curatorClient, "testLatchId", "test-service") {
            @Override
            public Collection<Participant> getParticipants() {
                return Collections.emptyList();
            }
        };
        LOG.trace("Created latch that always returns empty participants: {}", latch);
        return new LatchTestContext(latch, curatorClient, null);
    }

    private static LatchTestContext setupLatchThatThrowsGettingParticipants() {
        var curatorClient = newCuratorClient();
        var exception = new ManagedLeaderLatchException(new KeeperException.NoNodeException("/latch/path"));
        var latch = new ManagedLeaderLatch(curatorClient, "testLatchId", "test-service") {
            @Override
            public Collection<Participant> getParticipants() {
                throw exception;
            }
        };
        LOG.trace("Created latch that always throws an exception: {}", latch);
        return new LatchTestContext(latch, curatorClient, exception);
    }

    private static CuratorFramework newCuratorClient() {
        var retryPolicy = new RetryOneTime(500);
        return CuratorFrameworkFactory.newClient(ZK_TEST_SERVER.getConnectString(), retryPolicy);
    }

    private record LatchTestContext(ManagedLeaderLatch latch,
                                    CuratorFramework curatorClient,
                                    @Nullable Exception exception) {
    }

    private static void awaitIsLeader(ManagedLeaderLatch latch) {
        awaitLeadership(latch, true);
    }

    private static void awaitNotLeader(ManagedLeaderLatch latch) {
        awaitLeadership(latch, false);
    }

    private static void awaitLeadership(ManagedLeaderLatch latch, boolean isLeader) {
        await5SecondsUntilAsserted(() -> assertThat(latch.hasLeadership()).isEqualTo(isLeader));
    }
}
