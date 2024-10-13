package org.kiwiproject.curator.leader.health;

import static java.util.Collections.emptyList;
import static org.kiwiproject.test.assertj.dropwizard.metrics.HealthCheckResultAssertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.curator.framework.recipes.leader.Participant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.curator.leader.ManagedLeaderLatch;

import java.util.List;

@DisplayName("ManagedLeaderLatchHealthCheck")
class ManagedLeaderLatchHealthCheckTest {

    private static final boolean NOT_LEADER = false;
    private static final boolean IS_LEADER = true;

    private ManagedLeaderLatch leaderLatch;
    private ManagedLeaderLatchHealthCheck healthCheck;

    @BeforeEach
    void setUp() {
        leaderLatch = mock(ManagedLeaderLatch.class);
        when(leaderLatch.getLatchPath()).thenReturn("/some/path");
        healthCheck = new ManagedLeaderLatchHealthCheck(leaderLatch);
    }

    @Nested
    class WhenLatchIsStarted {

        @BeforeEach
        void setUp() {
            when(leaderLatch.isStarted()).thenReturn(true);
        }

        @Nested
        class WhenOnlyOneLeader {

            @BeforeEach
            void setUp() {
                var participant1 = new Participant("1", NOT_LEADER);
                var participant2 = new Participant("2", IS_LEADER);
                var participant3 = new Participant("3", NOT_LEADER);
                var participants = List.of(participant1, participant2, participant3);
                when(leaderLatch.getParticipants()).thenReturn(participants);
            }

            @Test
            void shouldBeHealthy_WhenNotLeader() {
                when(leaderLatch.getId()).thenReturn("1");
                when(leaderLatch.hasLeadership()).thenReturn(false);

                assertThat(healthCheck)
                        .isHealthy()
                        .hasMessage("Leader latch is started (has leadership? false)")
                        .hasDetail("leader", false)
                        .hasDetail("leaderParticipant", "2")
                        .hasDetail("thisParticipant", "1")
                        .hasDetail("participants", List.of("1", "2", "3"))
                        .hasDetail("severity", "OK");
                verify(leaderLatch).isStarted();
            }

            @Test
            void shouldBeHealthy_WhenIsTheLeader() {
                when(leaderLatch.getId()).thenReturn("2");
                when(leaderLatch.hasLeadership()).thenReturn(true);

                assertThat(healthCheck)
                        .isHealthy()
                        .hasMessage("Leader latch is started (has leadership? true)")
                        .hasDetail("leader", true)
                        .hasDetail("leaderParticipant", "2")
                        .hasDetail("thisParticipant", "2")
                        .hasDetail("participants", List.of("1", "2", "3"))
                        .hasDetail("severity", "OK");

                verify(leaderLatch).isStarted();
            }
        }

        @Nested
        class WhenNoLeaders {

            @Test
            void shouldBeUnhealthy() {
                when(leaderLatch.getId()).thenReturn("1");
                when(leaderLatch.getParticipants()).thenReturn(emptyList());

                assertThat(healthCheck)
                        .isUnhealthy()
                        .hasMessage("There are NO leaders for latch path /some/path")
                        .hasDetail("leader", false)
                        .hasDetail("leaderParticipant", null)
                        .hasDetail("thisParticipant", "1")
                        .hasDetail("participants", List.of())
                        .hasDetail("severity", "CRITICAL");

                verify(leaderLatch).isStarted();
            }
        }

        @Nested
        class WhenMoreThanOneLeader {

            @Test
            void shouldBeUnhealthy() {
                var participant1 = new Participant("1", NOT_LEADER);
                var participant2 = new Participant("2", IS_LEADER);
                var participant3 = new Participant("3", IS_LEADER);
                var participants = List.of(participant1, participant2, participant3);

                when(leaderLatch.getId()).thenReturn("1");
                when(leaderLatch.getParticipants()).thenReturn(participants);

                assertThat(healthCheck)
                        .isUnhealthy()
                        .hasMessage("There is more than one leader for latch path /some/path. Leader IDs: [2, 3] (this latch ID: 1)")
                        .hasDetail("leader", false)
                        .hasDetail("leaderParticipant", "2")
                        .hasDetail("thisParticipant", "1")
                        .hasDetail("participants", List.of("1", "2", "3"))
                        .hasDetail("severity", "CRITICAL");

                verify(leaderLatch).isStarted();
            }
        }
    }

    @Nested
    class WhenLatchNotStarted {

        @Test
        void shouldBeUnhealthy() {
            when(leaderLatch.getId()).thenReturn("1");
            when(leaderLatch.isStarted()).thenReturn(false);

            assertThat(healthCheck)
                    .isUnhealthy()
                    .hasMessageEndingWith("is not started")
                    .hasDetail("leader", false)
                    .hasDetail("leaderParticipant", null)
                    .hasDetail("thisParticipant", "1")
                    .hasDetail("participants", List.of())
                    .hasDetail("severity", "CRITICAL");
        }
    }
}
