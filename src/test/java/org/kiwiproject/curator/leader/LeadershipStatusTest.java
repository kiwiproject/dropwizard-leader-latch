package org.kiwiproject.curator.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.kiwiproject.curator.leader.LeadershipStatus.CuratorNotStarted;
import org.kiwiproject.curator.leader.LeadershipStatus.IsLeader;
import org.kiwiproject.curator.leader.LeadershipStatus.LatchNotStarted;
import org.kiwiproject.curator.leader.LeadershipStatus.NoLatchParticipants;
import org.kiwiproject.curator.leader.LeadershipStatus.NotLeader;
import org.kiwiproject.curator.leader.LeadershipStatus.OtherError;

@DisplayName("LeadershipStatus")
class LeadershipStatusTest {

    @Test
    void shouldCheckIsValidStatus() {
        assertAll(
                () -> assertThat(new IsLeader().isValidStatus()).isTrue(),
                () -> assertThat(new NotLeader().isValidStatus()).isTrue(),
                () -> assertThat(new CuratorNotStarted(CuratorFrameworkState.LATENT).isValidStatus()).isFalse(),
                () -> assertThat(new LatchNotStarted(LeaderLatch.State.CLOSED).isValidStatus()).isFalse(),
                () -> assertThat(new NoLatchParticipants().isValidStatus()).isFalse(),
                () -> assertThat(new OtherError(new KeeperException.NoNodeException("/latch/path")).isValidStatus()).isFalse()
        );
    }

    @Test
    void shouldCheckIsErrorStatus() {
        assertAll(
                () -> assertThat(new IsLeader().isErrorStatus()).isFalse(),
                () -> assertThat(new NotLeader().isErrorStatus()).isFalse(),
                () -> assertThat(new CuratorNotStarted(CuratorFrameworkState.STOPPED).isErrorStatus()).isTrue(),
                () -> assertThat(new LatchNotStarted(LeaderLatch.State.LATENT).isErrorStatus()).isTrue(),
                () -> assertThat(new NoLatchParticipants().isErrorStatus()).isTrue(),
                () -> assertThat(new OtherError(new KeeperException.NoNodeException("/latch/path")).isErrorStatus()).isTrue()
        );
    }

    @Nested
    class CuratorNotStartedRecord {

        @ParameterizedTest
        @NullSource
        @EnumSource(value = CuratorFrameworkState.class, names = { "STARTED" })
        void shouldNotAllowNullOrStartedState(CuratorFrameworkState curatorState) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new CuratorNotStarted(curatorState));
        }

        @ParameterizedTest
        @EnumSource(value = CuratorFrameworkState.class, names = { "STARTED" }, mode = EnumSource.Mode.EXCLUDE)
        void shouldAcceptValidStates(CuratorFrameworkState curatorState) {
            assertThatCode(() -> new CuratorNotStarted(curatorState)).doesNotThrowAnyException();
        }
    }

    @Nested
    class LatchNotStartedRecord {

        @ParameterizedTest
        @NullSource
        @EnumSource(value = LeaderLatch.State.class, names = { "STARTED" })
        void shouldNotAllowNullOrStartedState(LeaderLatch.State latchState) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new LatchNotStarted(latchState));
        }

        @ParameterizedTest
        @EnumSource(value = LeaderLatch.State.class, names = { "STARTED" }, mode = EnumSource.Mode.EXCLUDE)
        void shouldAcceptValidStates(LeaderLatch.State latchState) {
            assertThatCode(() -> new LatchNotStarted(latchState)).doesNotThrowAnyException();
        }
    }
}
