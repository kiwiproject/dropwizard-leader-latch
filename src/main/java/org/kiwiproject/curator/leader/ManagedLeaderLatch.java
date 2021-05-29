package org.kiwiproject.curator.leader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;

import com.google.common.base.MoreObjects;
import io.dropwizard.lifecycle.Managed;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.kiwiproject.curator.leader.exception.ManagedLeaderLatchException;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Wrapper around Curator's {@link LeaderLatch} which standardizes the id and latch path in ZooKeeper, and which
 * registers with Dropwizard so that it manages the lifecycle (mainly to ensure the latch is stopped when the
 * Dropwizard app stops).
 *
 * @implNote Any methods that use {@link #hasLeadership()} may result in a {@link ManagedLeaderLatchException} since
 * that method can potentially throw one. See its documentation for the possible situations in which an exception
 * could be thrown.
 */
@Slf4j
public class ManagedLeaderLatch implements Managed {

    private static final String ROOT_ZNODE_PATH = "/kiwi/leader-latch";

    @Getter
    private final String id;

    @Getter
    private final String latchPath;

    private final LeaderLatch leaderLatch;
    private final CuratorFramework client;
    private final AtomicBoolean started;

    /**
     * Construct a latch with a standard ID and latch path.
     *
     * @param client            the {@link CuratorFramework} client that this latch should use
     * @param serviceDescriptor service metadata
     * @param listeners         zero or more {@link LeaderLatchListener} instances to attach to the leader latch
     * @throws IllegalArgumentException if any arguments are null or blank
     */
    public ManagedLeaderLatch(CuratorFramework client,
                              ServiceDescriptor serviceDescriptor,
                              LeaderLatchListener... listeners) {
        this(client,
                leaderLatchId(serviceDescriptor),
                serviceDescriptor.getName(),
                listeners);
    }

    /**
     * Construct a latch with a specific ID and standard latch path.
     * <p>
     * The {@code serviceName} should be the generic name of a service, e.g. "payment-service" or "order-service",
     * instead of a unique service identifier.
     *
     * @param client      the {@link CuratorFramework} client that this latch should use
     * @param id          the unique ID for this latch instance
     * @param serviceName the generic name of the service, which ensures the same latch path is used for all
     *                    instances of a given service
     * @param listeners   zero or more {@link LeaderLatchListener} instances to attach to the leader latch
     * @throws IllegalArgumentException if any arguments are null or blank
     * @see #leaderLatchPath(String)
     */
    public ManagedLeaderLatch(CuratorFramework client,
                              String id,
                              String serviceName,
                              LeaderLatchListener... listeners) {
        this.client = requireNotNull(client);
        this.id = requireNotBlank(id);
        this.latchPath = leaderLatchPath(requireNotBlank(serviceName));
        this.leaderLatch = new LeaderLatch(client, latchPath, id, LeaderLatch.CloseMode.NOTIFY_LEADER);
        Arrays.stream(requireNonNull(listeners)).forEach(leaderLatch::addListener);
        this.started = new AtomicBoolean();
    }

    /**
     * Utility method to generate a standard latch id for a service.
     *
     * @param serviceDescriptor the service information to use
     * @return a latch ID
     */
    public static String leaderLatchId(ServiceDescriptor serviceDescriptor) {
        checkArgumentNotNull(serviceDescriptor);

        return leaderLatchId(
                serviceDescriptor.getName(),
                serviceDescriptor.getVersion(),
                serviceDescriptor.getHostname(),
                serviceDescriptor.getPort());
    }

    /**
     * Utility method to generate a standard latch id for a service.
     *
     * @param serviceName    the name of the service
     * @param serviceVersion the version of the service
     * @param hostname       the host name where the service instance is running
     * @param port           the port on which the service instance is running
     * @return a latch ID
     */
    public static String leaderLatchId(String serviceName,
                                       String serviceVersion,
                                       String hostname,
                                       int port) {
        return f("{}/{}/{}:{}", serviceName, serviceVersion, hostname, port);
    }

    /**
     * Utility method to generate a standard latch path for a service.
     *
     * @param serviceName the name of the service
     * @return the latch path for the given service
     */
    public static String leaderLatchPath(String serviceName) {
        return Paths.get(ROOT_ZNODE_PATH, serviceName, "leader-latch").toString();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("latchPath", latchPath)
                .add("started", started.get())
                .toString();
    }

    /**
     * Starts the latch, possibly creating non-existent znodes in ZooKeeper first.
     * <p>
     * The CuratorFramework must be started, or else a {@link com.google.common.base.VerifyException} will
     * be thrown.
     * <p>
     * This method will ignore repeated attempts to start once the latch has been started.
     *
     * @throws com.google.common.base.VerifyException if the {@link CuratorFramework} is not already started
     */
    @Override
    public void start() throws Exception {
        verify(client.getState() == CuratorFrameworkState.STARTED, "CuratorFramework must be started");

        if (started.compareAndSet(false, true)) {
            ensurePathsExistAndStartLatch();
        } else {
            LOG.trace("start() has already been called. Ignoring request to start");
        }
    }

    private void ensurePathsExistAndStartLatch() throws Exception {
        LOG.trace("Checking latch path {}", latchPath);
        var possiblyNullStat = checkPathExists(latchPath);
        var lockPathStat = Optional.ofNullable(possiblyNullStat).orElseGet(this::createLeaderLatchNode);
        LOG.info("Path [{}] creation time: {}",
                latchPath,
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(lockPathStat.getCtime()), ZoneOffset.UTC));
        LOG.trace("Starting leader latch {}", id);
        leaderLatch.start();
    }

    private Stat createLeaderLatchNode() {
        try {
            LOG.info("Path {} does not exist. Creating it.", latchPath);
            String path = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(latchPath);
            checkState(path.equals(latchPath), "Created path %s does not match expected path %s", path, latchPath);
            return checkPathExists(path);
        } catch (Exception e) {
            throw new ManagedLeaderLatchException("Unable to create latch path: " + latchPath, e);
        }
    }

    private Stat checkPathExists(String path) throws Exception {
        return client.checkExists().forPath(path);
    }

    /**
     * Stops the latch. Any exceptions closing the latch are ignored, although they are logged.
     */
    @Override
    public void stop() {
        LOG.trace("Stopping leader latch {}", id);
        try {
            leaderLatch.close();
        } catch (Exception e) {
            LOG.error("Error closing leader latch {}", id, e);
        }
    }

    /**
     * Returns whether this instance is the leader, or throws a {@link ManagedLeaderLatchException} if Curator is not
     * started yet, this latch is not started yet, or there are no latch participants yet.
     * <p>
     * The above mentioned situations could happen, for example, because code at startup calls this method before
     * Curator has been started, e.g. before the Jetty server starts in a Dropwizard application, or because the latch
     * does not yet have participants even though Curator and the latch are both started. These restrictions should
     * help prevent false negatives, i.e. having a {@code false} return value but the actual reason was because of
     * some other factor.
     *
     * @return true if this latch is currently the leader
     * @throws ManagedLeaderLatchException if this method is called and any of the restrictions mentioned above apply
     */
    public boolean hasLeadership() {
        if (client.getState() != CuratorFrameworkState.STARTED) {
            logAndThrowLatchException("Curator must be started before calling this method. Curator state: " + client.getState());
        }
        if (!isStarted()) {
            logAndThrowLatchException("LeaderLatch must be started before calling this method. Latch state: " + leaderLatch.getState());
        }
        if (getParticipants().isEmpty()) {
            logAndThrowLatchException("LeaderLatch must have participants before calling this method.");
        }
        boolean isLeader = leaderLatch.hasLeadership();
        LOG.trace("hasLeadership? {}", isLeader);
        return isLeader;
    }

    private void logAndThrowLatchException(String msg) {
        var exception = new ManagedLeaderLatchException(msg);
        LOG.warn(msg);
        LOG.debug("Stack trace: ", exception);
        throw exception;
    }

    /**
     * Get the participants (i.e. Dropwizard services) in this latch.
     *
     * @return unordered collection of leader latch participants
     * @throws ManagedLeaderLatchException if any error occurs getting the participants
     */
    public Collection<Participant> getParticipants() {
        try {
            return leaderLatch.getParticipants();
        } catch (Exception e) {
            throw new ManagedLeaderLatchException(e);
        }
    }

    /**
     * Get the leader of this latch.
     *
     * @return the {@link Participant} who is the current leader
     * @throws ManagedLeaderLatchException if any error occurs getting the leader
     */
    public Participant getLeader() {
        try {
            return leaderLatch.getLeader();
        } catch (Exception e) {
            throw new ManagedLeaderLatchException(e);
        }
    }

    /**
     * Check if the latch is started.
     *
     * @return true if the latch state is {@link LeaderLatch.State#STARTED}
     */
    public boolean isStarted() {
        return leaderLatch.getState() == LeaderLatch.State.STARTED;
    }

    /**
     * Check if the latch is closed.
     *
     * @return true if the latch state is {@link LeaderLatch.State#CLOSED}
     */
    public boolean isClosed() {
        return leaderLatch.getState() == LeaderLatch.State.CLOSED;
    }

    /**
     * Get the current latch state.
     *
     * @return the current {@link LeaderLatch.State}
     */
    public LeaderLatch.State getLatchState() {
        return leaderLatch.getState();
    }

    /**
     * Perform the given {@code action} <em>synchronously</em> only if this latch is currently the leader. Use this
     * when the action does not need to return a value and it is a "fire and forget" action.
     *
     * @param action the action to perform if this latch is the leader
     */
    public void whenLeader(Runnable action) {
        if (hasLeadership()) {
            action.run();
        }
    }

    /**
     * Perform the given {@code action} <em>asynchronously</em> only if this latch is currently the leader. Use this
     * when the action does not need to return a value and it is a "fire and forget" action. However, if the
     * returned {@link Optional} is present, you can use the {@link CompletableFuture} to determine when the action
     * has completed and take some other action, etc. if you want to.
     *
     * @param action the action to perform if this latch is the leader
     * @return an Optional containing a CompletableFuture if this latch is the leader, otherwise an empty Optional
     */
    public Optional<CompletableFuture<Void>> whenLeaderAsync(Runnable action) {
        if (hasLeadership()) {
            return Optional.of(CompletableFuture.runAsync(action));
        }

        return Optional.empty();
    }

    /**
     * Perform the given action defined by {@code resultSupplier} <em>synchronously</em> only if this latch is
     * currently the leader, returning the result of {@code resultSupplier}.
     *
     * @param resultSupplier the result-returning action to perform if this latch is the leader
     * @param <T>            the result type
     * @return an Optional containing the result if this latch is the leader, otherwise an empty Optional
     */
    public <T> Optional<T> whenLeader(Supplier<T> resultSupplier) {
        if (hasLeadership()) {
            return Optional.of(resultSupplier.get());
        }

        return Optional.empty();
    }

    /**
     * Perform the given action defined by {@code resultSupplier} <em>asynchronously</em> only if this latch is
     * currently the leader, returning a {@link CompletableFuture} whose result will be the result of the
     * {@code resultSupplier}.
     *
     * @param resultSupplier the result-returning action to perform if this latch is the leader
     * @param <T>            the result type
     * @return an Optional containing a CompletableFuture if this latch is the leader, otherwise an empty Optional
     */
    public <T> Optional<CompletableFuture<T>> whenLeaderAsync(Supplier<T> resultSupplier) {
        if (hasLeadership()) {
            return Optional.of(CompletableFuture.supplyAsync(resultSupplier));
        }

        return Optional.empty();
    }

    /**
     * Same as {@link #whenLeaderAsync(Supplier)} but uses supplied {@code executor} instead of
     * {@link CompletableFuture}'s default executor.
     *
     * @param resultSupplier the result-returning action to perform if this latch is the leader
     * @param executor       the custom {@link Executor} to use
     * @param <T>            the result type
     * @return an Optional containing a CompletableFuture if this latch is the leader, otherwise an empty Optional
     */
    public <T> Optional<CompletableFuture<T>> whenLeaderAsync(Supplier<T> resultSupplier, Executor executor) {
        if (hasLeadership()) {
            return Optional.of(CompletableFuture.supplyAsync(resultSupplier, executor));
        }

        return Optional.empty();
    }
}
