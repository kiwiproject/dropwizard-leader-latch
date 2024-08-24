package org.kiwiproject.curator.leader;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

/**
 * Defines a type that can be returned when checking if a {@link LeaderLatch}
 * has leadership, taking into account the various issues that can cause problems.
 * <p>
 * There are two sub-interfaces, {@link ValidLeadershipStatus} and
 * {@link ErrorLeadershipStatus}, each of which has several {@code record} implementations.
 * This allows you to determine whether the return value is a valid status or if
 * there is some kind of error.
 */
public sealed interface LeadershipStatus {

    /**
     * Check whether the status is from a valid state, where the
     * Curator client and the {@code LeaderLatch} are both started, and there
     * are latch participants.
     *
     * @return true if the leadership status is from a valid latch,
     * otherwise false
     */
    default boolean isValidStatus() {
        return !isErrorStatus();
    }

    /**
     * Check whether the status is from a bad latch state, where
     * the Curator client or the {@code LeaderLatch} might not be started
     * or there are no participants in the latch.
     *
     * @return true if the leadership status is from an invalid latch state,
     * otherwise false
     */
    boolean isErrorStatus();

    /**
     * Defines a valid leadership status.
     */
    sealed interface ValidLeadershipStatus extends LeadershipStatus
            permits IsLeader, NotLeader {

        @Override
        default boolean isErrorStatus() {
            return false;
        }
    }

    /**
     * Defines an invalid leadership status caused by an error or erroneous state.
     */
    sealed interface ErrorLeadershipStatus extends LeadershipStatus
            permits CuratorNotStarted, LatchNotStarted, NoLatchParticipants, OtherError {

        @Override
        default boolean isErrorStatus() {
            return true;
        }
    }

    /**
     * Represents a valid latch state, where the participant is the current leader.
     */
    record IsLeader() implements ValidLeadershipStatus {}

    /**
     * Represents a valid latch state, where the participant is not the leader.
     */
    record NotLeader() implements ValidLeadershipStatus {}

    /**
     * Represents an invalid latch state; Curator is not in the
     * {@link CuratorFrameworkState#STARTED STARTED} state.
     *
     * @param curatorState the Curator state
     */
    record CuratorNotStarted(CuratorFrameworkState curatorState) implements ErrorLeadershipStatus {
        public CuratorNotStarted {
            checkArgument(nonNull(curatorState) && curatorState != CuratorFrameworkState.STARTED,
                    "curatorState must not be null or STARTED");
        }
    }

    /**
     * Represents an invalid latch state; the LeaderLatch is not in the
     * {@link LeaderLatch.State#STARTED STARTED} state.
     *
     * @param latchState the {@code LeaderLatch} state
     */
    record LatchNotStarted(LeaderLatch.State latchState) implements ErrorLeadershipStatus {
        public LatchNotStarted {
            checkArgument(nonNull(latchState) && latchState != LeaderLatch.State.STARTED,
                    "latchState must not be null or STARTED");
        }
    }

    /**
     * Represents an invalid latch state; the {@code LeaderLatch} has no participants.
     */
    record NoLatchParticipants() implements ErrorLeadershipStatus {}

    /**
     * Represents an invalid latch state; an unexpected Exception was thrown.
     *
     * @param error the exception that was thrown checking leadership status
     */
    record OtherError(Exception error) implements ErrorLeadershipStatus {
        public OtherError {
            checkArgumentNotNull(error, "error must not be null");
        }
    }
}
