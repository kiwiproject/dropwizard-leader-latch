package org.kiwiproject.curator.leader;

import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.collect.KiwiLists.second;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.curator.leader.health.ManagedLeaderLatchHealthCheck;
import org.kiwiproject.curator.leader.resource.GotLeaderLatchResource;
import org.kiwiproject.curator.leader.resource.LeaderResource;
import org.kiwiproject.test.curator.CuratorTestingServerExtension;
import org.kiwiproject.test.dropwizard.mockito.DropwizardMockitoMocks;
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

    @BeforeEach
    void setUp() {
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
    void tearDown() throws Exception {
        var rootPath = "/kiwi/leader-latch";

        if (pathExists(rootPath)) {
            LOG.debug("Path {} exists, attempting to delete it", rootPath);
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath(rootPath);

            // In GitHub we have intermittent test failures caused by NodeExistsException thrown in setUp.
            // The following attempts to wait and see if it gets deleted. Also, added guaranteed() in the
            // above code that attempts the deletion, which causes Curator to attempt background deletes.
            // See issue: https://github.com/kiwiproject/dropwizard-leader-latch/issues/36
            if (pathExists(rootPath)) {
                LOG.warn("Path {} still exists; wait up to five seconds for it to be deleted", rootPath);
                await().atMost(FIVE_SECONDS).until(() -> !pathExists(rootPath));
            }
        }
    }

    private boolean pathExists(String rootPath) throws Exception {
        var stat = client.checkExists().forPath(rootPath);

        return nonNull(stat);
    }

    @Test
    void shouldThrowIllegalStateExceptions_FromGetMethods_WhenNotStarted(SoftAssertions softly) {
        var latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor);

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
        var latchCreator = ManagedLeaderLatchCreator
                .from(client, environment, serviceDescriptor)
                .start();

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

    @Test
    void shouldIgnoreMultipleCallsToStart() {
        var latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor);

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
        var latchCreator = ManagedLeaderLatchCreator
                .from(client, environment, serviceDescriptor, noOpListener)
                .start();

        assertThat(latchCreator.getListeners()).hasSize(1);
        assertThat(first(latchCreator.getListeners())).isSameAs(noOpListener);
    }

    @Test
    void shouldRegisterListeners_InOrder_UsingAddMethod() {
        var noOpListener = new NoOpListener();
        var loggingListener = new SimpleLoggingListener();
        var latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor)
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
        var latchCreator = ManagedLeaderLatchCreator
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
        var latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor).start();
        assertThat(latchCreator.getHealthCheck()).isNotEmpty();

        var healthCheck = latchCreator.getHealthCheck().orElseThrow(IllegalStateException::new);
        verify(healthCheckRegistry).register("leaderLatch", healthCheck);
    }

    @Test
    void shouldNotRegisterHeathCheck_IfDisabled() {
        var latchCreator = ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor)
                .withoutHealthCheck()
                .start();

        assertThat(latchCreator.getHealthCheck()).isEmpty();
        verifyNoInteractions(healthCheckRegistry);
    }

    @Test
    void shouldRegisterResources_ByDefault() {
        ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor).start();

        verify(jersey).register(isA(GotLeaderLatchResource.class));
        verify(jersey).register(isA(LeaderResource.class));
    }

    @Test
    void shouldNotRegisterResources_IfDisabled() {
        ManagedLeaderLatchCreator.from(client, environment, serviceDescriptor)
                .withoutResources()
                .start();

        verifyNoInteractions(jersey);
    }

    @Test
    void shouldStartLeaderLatch_WithAllTheDefaultThings() {
        var listener = new BecameLeaderListener();
        var leaderLatch = ManagedLeaderLatchCreator.startLeaderLatch(client, environment, serviceDescriptor, listener);

        verify(healthCheckRegistry).register(eq("leaderLatch"), isA(ManagedLeaderLatchHealthCheck.class));
        verify(jersey).register(isA(GotLeaderLatchResource.class));
        verify(jersey).register(isA(LeaderResource.class));

        assertStartedAndBecameLeader(leaderLatch, listener);
    }

    @Test
    void shouldStart_WithAllTheDefaultThings() {
        var listener = new BecameLeaderListener();
        var latchCreator = ManagedLeaderLatchCreator.start(client, environment, serviceDescriptor, listener);

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
