package org.kiwiproject.curator.leader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.kiwiproject.curator.leader.exception.ManagedLeaderLatchException;
import org.kiwiproject.curator.leader.health.ManagedLeaderLatchHealthCheck;
import org.kiwiproject.curator.leader.resource.GotLeaderLatchResource;
import org.kiwiproject.curator.leader.resource.LeaderResource;

import java.util.List;
import java.util.Optional;

/**
 * Entry point to initialize {@link ManagedLeaderLatch}, which wraps an Apache Curator
 * {@link org.apache.curator.framework.recipes.leader.LeaderLatch} and allows for easy determination
 * whether a JVM process is the leader in a group of JVMs connected to a ZooKeeper cluster.
 * <p>
 * The {@link ManagedLeaderLatch} created by this class will be started immediately, but is a Dropwizard
 * {@link io.dropwizard.lifecycle.Managed} so that it will be stopped when the Dropwizard service shuts down.
 * <p>
 * In addition, by default a {@link ManagedLeaderLatchHealthCheck} is registered with Dropwizard. Two REST resources
 * are registered, {@link GotLeaderLatchResource} and {@link LeaderResource}.
 */
@Slf4j
public class ManagedLeaderLatchCreator {

    // Initialized at construction
    private final CuratorFramework client;
    private final Environment environment;
    private final ServiceDescriptor serviceDescriptor;
    private final List<LeaderLatchListener> listeners;

    // Initialized via instance start() method so cannot be final (unless we used Kotlin and lateinit)
    private ManagedLeaderLatch leaderLatch;
    private boolean addHealthCheck;
    private ManagedLeaderLatchHealthCheck healthCheck;
    private boolean addResources;

    private ManagedLeaderLatchCreator(CuratorFramework client,
                                      Environment environment,
                                      ServiceDescriptor serviceDescriptor,
                                      List<LeaderLatchListener> listeners) {

        this.client = requireNotNull(client);
        checkState(client.getState() == CuratorFrameworkState.STARTED, "CuratorFramework must be started");

        this.environment = requireNotNull(environment);
        this.serviceDescriptor = requireNotNull(serviceDescriptor);
        this.listeners = requireNotNull(listeners);
        this.addHealthCheck = true;
        this.addResources = true;
    }

    /**
     * Static factory method to create a {@link ManagedLeaderLatchCreator}.
     * <p>
     * Use this method when you want to perform additional configuration before starting the leader latch.
     * <p>
     * You will need to call {@link #start()} on the returned instance in order to start the
     * {@link ManagedLeaderLatch}.
     *
     * @param client            the Curator client
     * @param environment       the Dropwizard environment
     * @param serviceDescriptor service metadata
     * @param listeners         optional listeners
     * @return a new instance
     * @throws IllegalStateException if the {@code client} is not started, or any required arguments are null
     */
    public static ManagedLeaderLatchCreator from(CuratorFramework client,
                                                 Environment environment,
                                                 ServiceDescriptor serviceDescriptor,
                                                 LeaderLatchListener... listeners) {

        // The list of listeners passed to the constructor must be mutable or else exceptions will be thrown if
        // addLeaderLatchListener is called subsequently (since it adds to the existing list).

        return new ManagedLeaderLatchCreator(client, environment, serviceDescriptor, newArrayList(listeners));
    }

    /**
     * If the only thing you want is a {@link ManagedLeaderLatch} and you want the standard options (a health
     * check and JAX-RS REST resources) and you do not need references to them, use this method to create and
     * start a latch.
     * <p>
     * Otherwise use {@link #start(CuratorFramework, Environment, ServiceDescriptor, LeaderLatchListener...)}.
     *
     * @param client            the Curator client
     * @param environment       the Dropwizard environment
     * @param serviceDescriptor the service information from the registry
     * @param listeners         optional listeners
     * @return a started {@link ManagedLeaderLatch}
     * @throws IllegalStateException if the {@code client} is not started, or any required arguments are null
     * @see #start(CuratorFramework, Environment, ServiceDescriptor, LeaderLatchListener...)
     */
    public static ManagedLeaderLatch startLeaderLatch(CuratorFramework client,
                                                      Environment environment,
                                                      ServiceDescriptor serviceDescriptor,
                                                      LeaderLatchListener... listeners) {

        var leaderLatchCreator = start(client, environment, serviceDescriptor, listeners);

        return leaderLatchCreator.getLeaderLatch();
    }

    /**
     * If you want a {@link ManagedLeaderLatch} and you want the standard options (a health
     * check and JAX-RS REST resources) and you might need references to them, use this method to create and
     * start a latch.
     * <p>
     * The returned {@link ManagedLeaderLatchCreator} can be used to then obtain the {@link ManagedLeaderLatch}
     * as well as the health check and listeners.
     *
     * @param client      the Curator client
     * @param environment the Dropwizard environment
     * @param serviceInfo the service information from the registry
     * @param listeners   optional listeners
     * @return a {@link ManagedLeaderLatchCreator} with a started {@link ManagedLeaderLatch}
     * @throws IllegalStateException if the {@code client} is not started, or any required arguments are null
     */
    public static ManagedLeaderLatchCreator start(CuratorFramework client,
                                                  Environment environment,
                                                  ServiceDescriptor serviceInfo,
                                                  LeaderLatchListener... listeners) {

        return from(client, environment, serviceInfo, listeners).start();
    }

    /**
     * Configures <em>without</em> a health check.
     * <p>
     * Use only when constructing a new {@link ManagedLeaderLatchCreator}.
     *
     * @return this instance, for method chaining
     */
    public ManagedLeaderLatchCreator withoutHealthCheck() {
        addHealthCheck = false;
        return this;
    }

    /**
     * Configures <em>without</em> REST resources to check for leadership and if a leader latch is present.
     * <p>
     * Use only when constructing a new {@link ManagedLeaderLatchCreator}.
     *
     * @return this instance, for method chaining
     */
    public ManagedLeaderLatchCreator withoutResources() {
        addResources = false;
        return this;
    }

    /**
     * Adds the specified {@link LeaderLatchListener}.
     * <p>
     * Use only when constructing a new {@link ManagedLeaderLatchCreator}.
     *
     * @param listener the listener to add
     * @return this instance, for method chaining
     */
    public ManagedLeaderLatchCreator addLeaderLatchListener(LeaderLatchListener listener) {
        listeners.add(listener);
        return this;
    }

    /**
     * Starts the leader latch, performing the following actions:
     * <ul>
     * <li>
     *     Creates a new {@link ManagedLeaderLatch}, starts it, and tells the Dropwizard lifecycle to manage (stop) it
     * </li>
     * <li>
     *     Creates and registers a {@link ManagedLeaderLatchHealthCheck} unless explicitly disabled via
     *     {@link #withoutHealthCheck()}
     * </li>
     * <li>
     *     Creates and registers the JAX-RS REST endpoints unless explicitly disabled via {@link #withoutResources()}
     * </li>
     * </ul>
     * <p>
     * Note that once this method is called, nothing about the {@link ManagedLeaderLatch} can be changed, and calls
     * to other instance methods (e.g. addLeaderLatchListener) will have no effect. Similarly, calling this method
     * more than once is considered unexpected behavior, and we will simply return the existing instance without
     * taking any other actions.
     *
     * @return this instance, from which you can then retrieve the (started) {@link ManagedLeaderLatch}
     * @throws ManagedLeaderLatchException if an error occurs starting the latch
     */
    public ManagedLeaderLatchCreator start() {
        if (nonNull(leaderLatch)) {
            LOG.warn("start() has already been called. Ignoring this invocation.");
            return this;
        }

        var listenerArray = listeners.toArray(new LeaderLatchListener[0]);
        leaderLatch = new ManagedLeaderLatch(client, serviceDescriptor, listenerArray);
        environment.lifecycle().manage(leaderLatch);
        startLatchOrThrow();
        addHealthCheckIfConfigured();
        addResourcesIfConfigured();

        return this;
    }

    private void startLatchOrThrow() {
        try {
            leaderLatch.start();
        } catch (Exception e) {
            throw new ManagedLeaderLatchException("Error starting leader latch", e);
        }
    }

    private void addHealthCheckIfConfigured() {
        if (addHealthCheck) {
            healthCheck = new ManagedLeaderLatchHealthCheck(leaderLatch);
            environment.healthChecks().register("leaderLatch", healthCheck);
        }
    }

    private void addResourcesIfConfigured() {
        if (addResources) {
            environment.jersey().register(new GotLeaderLatchResource());
            environment.jersey().register(new LeaderResource(leaderLatch));
        }
    }

    /**
     * Returns the {@link ManagedLeaderLatch} created after {@link #start()} has been called.
     *
     * @return the leader latch
     * @throws IllegalStateException if called but the latch has not been started yet
     */
    public ManagedLeaderLatch getLeaderLatch() {
        validateStarted();
        return leaderLatch;
    }

    /**
     * Returns the health check (if registered) after {@link #start()} has been called.
     *
     * @return the registered health check
     * @throws IllegalStateException if called but the latch has not been started yet
     */
    public Optional<ManagedLeaderLatchHealthCheck> getHealthCheck() {
        validateStarted();
        return Optional.ofNullable(healthCheck);
    }

    /**
     * Returns a list containing all registered {@link LeaderLatchListener}s after {@link #start()} has been called.
     *
     * @return any registered {@link LeaderLatchListener}s
     * @throws IllegalStateException if called but the latch has not been started yet
     * @implNote The returned list is an unmodifiable list containing the registered listeners
     */
    public List<LeaderLatchListener> getListeners() {
        validateStarted();
        return List.copyOf(listeners);
    }

    private void validateStarted() {
        checkState(nonNull(leaderLatch), "Leader latch is not started; call start() first");
    }
}
