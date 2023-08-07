package org.kiwiproject.curator.leader;

import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.collect.KiwiLists.second;
import static org.kiwiproject.curator.leader.util.CuratorTestHelpers.closeIfStarted;
import static org.kiwiproject.curator.leader.util.CuratorTestHelpers.deleteRecursivelyIfExists;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.ThrowableAssert;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.curator.leader.exception.ManagedLeaderLatchException;
import org.kiwiproject.curator.leader.health.ManagedLeaderLatchHealthCheck;
import org.kiwiproject.curator.leader.resource.GotLeaderLatchResource;
import org.kiwiproject.curator.leader.resource.LeaderResource;
import org.kiwiproject.curator.leader.util.CuratorTestHelpers;
import org.kiwiproject.test.curator.CuratorTestingServerExtension;
import org.kiwiproject.test.dropwizard.mockito.DropwizardMockitoMocks;
import org.kiwiproject.test.junit.jupiter.ClearBoxTest;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@DisplayName("ManagedLeaderLatchCreator")
@ExtendWith(SoftAssertionsExtension.class)
@Slf4j
class ManagedLeaderLatchCreatorTest {

    @RegisterExtension
    static final CuratorTestingServerExtension ZK_TEST_SERVER = new CuratorTestingServerExtension();

    private CuratorFramework client;
    private Environment environment;
    private JerseyEnvironment jersey;
    private LifecycleEnvironment lifecycle;
    private HealthCheckRegistry healthCheckRegistry;
    private ServiceDescriptor serviceDescriptor;

    private ManagedLeaderLatchCreator latchCreator;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        LOG.info("Set up for test: {}", testInfo.getDisplayName());

        client = ZK_TEST_SERVER.getClient();
        var dropwizardMockitoContext = DropwizardMockitoMocks.mockDropwizard();
        environment = dropwizardMockitoContext.environment();
        jersey = dropwizardMockitoContext.jersey();
        lifecycle = dropwizardMockitoContext.lifecycle();
        healthCheckRegistry = dropwizardMockitoContext.healthChecks();
        serviceDescriptor = ServiceDescriptor.builder()
                .name("test-service")
                .version("42.0.84")
                .hostname("host42")
                .port(8042)
                .build();
    }

    @AfterEach
    void tearDown(TestInfo testInfo) throws Exception {
        LOG.info("Tear down after test: {}", testInfo.getDisplayName());

        if (nonNull(latchCreator) && latchCreator.isLeaderLatchStarted()) {
            closeIfStarted(latchCreator.getLeaderLatch());
        }

        var rootPath = "/kiwi/leader-latch";
        var deleteResult = deleteRecursivelyIfExists(client, rootPath);

        if (deleteResult.failed()) {
            LOG.error("Path {} was not deleted!", rootPath);

            var kiwiPath = "/kiwi";
            var childPaths = client.getChildren().forPath(kiwiPath);
            LOG.info("Children at {}: {}", kiwiPath, childPaths);
        } else {
            LOG.info("Path {} delete result: {}", rootPath, deleteResult);
        }
    }

    @Test
    void shouldThrowIllegalStateExceptions_FromGetMethods_WhenNotStarted(SoftAssertions softly) {
        latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor);
        assertThat(latchCreator.isLeaderLatchStarted()).isFalse();

        softlyAssertIllegalStateExceptionThrownBy(softly, latchCreator::getLeaderLatch);
        softlyAssertIllegalStateExceptionThrownBy(softly, latchCreator::getHealthCheck);
        softlyAssertIllegalStateExceptionThrownBy(softly, latchCreator::getListeners);
    }

    private void softlyAssertIllegalStateExceptionThrownBy(SoftAssertions softly,
                                                           ThrowableAssert.ThrowingCallable callable) {
        softly.assertThatThrownBy(callable).isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldCreateAndManageLeaderLatch(SoftAssertions softly) {
        latchCreator = ManagedLeaderLatchCreator
                .from(client, environment, serviceDescriptor)
                .start();
        assertThat(latchCreator.isLeaderLatchStarted()).isTrue();

        assertThat(latchCreator.getLeaderLatch()).isNotNull();
        softly.assertThat(latchCreator.getListeners()).isEmpty();

        var managedCaptor = ArgumentCaptor.forClass(Managed.class);
        verify(lifecycle).manage(managedCaptor.capture());
        var capturedArgs = managedCaptor.getAllValues();
        assertThat(capturedArgs).hasSize(1);

        var managed = first(capturedArgs);
        assertThat(managed).isExactlyInstanceOf(ManagedLeaderLatch.class);

        var managedLeaderLatch = (ManagedLeaderLatch) managed;
        softly.assertThat(managedLeaderLatch.getId())
                .isEqualTo(ManagedLeaderLatch.leaderLatchId(serviceDescriptor));

        softly.assertThat(managedLeaderLatch.getLatchPath())
                .isEqualTo(ManagedLeaderLatch.leaderLatchPath(serviceDescriptor.getName()));

        softly.assertThat(managedLeaderLatch.getLatchState())
                .isEqualTo(LeaderLatch.State.STARTED);
    }

    @ClearBoxTest
    void shouldThrowManagedLeaderLatchException_WhenLatchFailsToStart() throws Exception {
        var leaderLatch = mock(ManagedLeaderLatch.class);

        var ex = new RuntimeException("oop");
        doThrow(ex).when(leaderLatch).start();

        assertThatThrownBy(() -> ManagedLeaderLatchCreator.startLatchOrThrow(leaderLatch))
                .isExactlyInstanceOf(ManagedLeaderLatchException.class)
                .hasMessage("Error starting leader latch")
                .hasCause(ex);
    }

    @Test
    void shouldIgnoreMultipleCallsToStart() {
        latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor);

        var latch1 = latchCreator.start();
        var latch2 = latchCreator.start();
        var latch3 = latchCreator.start();

        assertThat(latch1)
                .isSameAs(latch2)
                .isSameAs(latch3);

        verify(lifecycle).manage(any(Managed.class));
        verify(jersey, times(2)).register(any(Object.class));
        verify(healthCheckRegistry).register(anyString(), any());

        verifyNoMoreInteractions(lifecycle, jersey, healthCheckRegistry);
    }

    @Test
    void shouldRegisterListeners_InOrder_UsingVarargs() {
        var noOpListener = new NoOpListener();
        latchCreator = ManagedLeaderLatchCreator
                .from(client, environment, serviceDescriptor, noOpListener)
                .start();

        assertThat(latchCreator.getListeners()).hasSize(1);
        assertThat(first(latchCreator.getListeners())).isSameAs(noOpListener);
    }

    @Test
    void shouldRegisterListeners_InOrder_UsingAddMethod() {
        var noOpListener = new NoOpListener();
        var loggingListener = new SimpleLoggingListener();
        latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor)
                .addLeaderLatchListener(noOpListener)
                .addLeaderLatchListener(loggingListener)
                .start();

        assertThat(latchCreator.getListeners()).hasSize(2);
        assertThat(first(latchCreator.getListeners())).isSameAs(noOpListener);
        assertThat(second(latchCreator.getListeners())).isSameAs(loggingListener);
    }

    @Test
    void shouldReturnImmutableCopyOfListeners() {
        var noOpListener = new NoOpListener();
        var loggingListener = new SimpleLoggingListener();
        latchCreator = ManagedLeaderLatchCreator
                .from(client, environment, serviceDescriptor, noOpListener, loggingListener)
                .start();

        var listeners = latchCreator.getListeners();

        assertThatThrownBy(() -> listeners.add(loggingListener))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static class NoOpListener implements LeaderLatchListener {
        @Override
        public void isLeader() {
        }

        @Override
        public void notLeader() {
        }
    }

    @Slf4j
    private static class SimpleLoggingListener implements LeaderLatchListener {
        @Override
        public void isLeader() {
            LOG.info("I lead");
        }

        @Override
        public void notLeader() {
            LOG.info("I follow");
        }
    }

    @Test
    void shouldRegisterHeathCheck_ByDefault() {
        latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor).start();
        assertThat(latchCreator.getHealthCheck()).isNotEmpty();

        var healthCheck = latchCreator.getHealthCheck().orElseThrow(IllegalStateException::new);
        verify(healthCheckRegistry).register("leaderLatch", healthCheck);
    }

    @Test
    void shouldNotRegisterHeathCheck_IfDisabled() {
        latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor)
                .withoutHealthCheck()
                .start();

        assertThat(latchCreator.getHealthCheck()).isEmpty();
        verifyNoInteractions(healthCheckRegistry);
    }

    @Test
    void shouldRegisterResources_ByDefault() {
        latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor).start();

        verify(jersey).register(isA(GotLeaderLatchResource.class));
        verify(jersey).register(isA(LeaderResource.class));
    }

    @Test
    void shouldNotRegisterResources_IfDisabled() {
        latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor)
                .withoutResources()
                .start();

        verifyNoInteractions(jersey);
    }

    @Test
    void shouldStartLeaderLatch_WithAllTheDefaultThings() {
        var listener = new BecameLeaderListener();
        ManagedLeaderLatch leaderLatch = null;
        try {
            leaderLatch = ManagedLeaderLatchCreator.startLeaderLatch(client, environment, serviceDescriptor, listener);

            verify(healthCheckRegistry).register(eq("leaderLatch"), isA(ManagedLeaderLatchHealthCheck.class));
            verify(jersey).register(isA(GotLeaderLatchResource.class));
            verify(jersey).register(isA(LeaderResource.class));

            assertStartedAndBecameLeader(leaderLatch, listener);
        } finally {
            CuratorTestHelpers.closeIfStarted(leaderLatch);
        }
    }

    @Test
    void shouldStart_WithAllTheDefaultThings() {
        var listener = new BecameLeaderListener();
        latchCreator = ManagedLeaderLatchCreator.start(client, environment, serviceDescriptor, listener);

        assertThat(latchCreator.getHealthCheck()).isPresent();
        assertThat(latchCreator.getListeners()).hasSize(1).hasOnlyElementsOfTypes(BecameLeaderListener.class);

        verify(healthCheckRegistry).register(eq("leaderLatch"), isA(ManagedLeaderLatchHealthCheck.class));
        verify(jersey).register(isA(GotLeaderLatchResource.class));
        verify(jersey).register(isA(LeaderResource.class));

        var leaderLatch = latchCreator.getLeaderLatch();
        assertStartedAndBecameLeader(leaderLatch, listener);
    }

    @Slf4j
    static class BecameLeaderListener implements LeaderLatchListener {

        AtomicBoolean becameLeader = new AtomicBoolean();

        @Override
        public void isLeader() {
            boolean result = becameLeader.compareAndSet(false, true);
            LOG.info("Became leader? {}", result);
        }

        @Override
        public void notLeader() {
            // no-op
        }
    }

    private void assertStartedAndBecameLeader(ManagedLeaderLatch leaderLatch, BecameLeaderListener listener) {
        assertThat(leaderLatch.isStarted()).isTrue();
        await().atMost(5, TimeUnit.SECONDS).until(() -> listener.becameLeader.get());
    }
}
