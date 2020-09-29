package org.kiwiproject.curator.leader.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.kiwiproject.jaxrs.KiwiGenericTypes.MAP_OF_STRING_TO_OBJECT_GENERIC_TYPE;
import static org.kiwiproject.test.jaxrs.JaxrsTestHelper.assertOkResponse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.curator.leader.ManagedLeaderLatch;
import org.kiwiproject.jaxrs.exception.IllegalArgumentExceptionMapper;
import org.kiwiproject.jaxrs.exception.JaxrsExceptionMapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@ExtendWith(DropwizardExtensionsSupport.class)
@DisplayName("LeaderResource")
class LeaderResourceTest {

    private static final ManagedLeaderLatch LEADER_LATCH = mock(ManagedLeaderLatch.class);

    private static final ResourceExtension RESOURCE = ResourceExtension.builder()
            .bootstrapLogging(false)
            .addProvider(JaxrsExceptionMapper.class)
            .addProvider(IllegalArgumentExceptionMapper.class)
            .addResource(new LeaderResource(LEADER_LATCH))
            .build();

    @AfterEach
    void tearDown() {
        reset(LEADER_LATCH);
    }

    @ParameterizedTest(name = "[{index}] (isLeader = {argumentsWithNames})")
    @ValueSource(booleans = {true, false})
    void shouldReportLeadership(boolean isLeader) {
        when(LEADER_LATCH.hasLeadership()).thenReturn(isLeader);

        var response = RESOURCE.client()
                .target("/kiwi/leader-latch/leader")
                .request()
                .get();
        assertOkResponse(response);

        var entity = response.readEntity(MAP_OF_STRING_TO_OBJECT_GENERIC_TYPE);
        assertThat(entity).containsOnly(entry("leader", isLeader));

        verify(LEADER_LATCH).hasLeadership();
    }

    @Test
    void testGetLatchState() {
        when(LEADER_LATCH.getId()).thenReturn("test-id-1");
        when(LEADER_LATCH.hasLeadership()).thenReturn(false);
        when(LEADER_LATCH.getLatchPath()).thenReturn("/the/true/path");
        when(LEADER_LATCH.getParticipants()).thenReturn(sampleParticipants());
        when(LEADER_LATCH.getLatchState()).thenReturn(LeaderLatch.State.STARTED);

        var response = RESOURCE.client()
                .target("/kiwi/leader-latch/latch")
                .request()
                .get();
        assertOkResponse(response);

        var entity = response.readEntity(MAP_OF_STRING_TO_OBJECT_GENERIC_TYPE);
        assertThat(entity)
                .hasSize(5)
                .containsEntry("id", "test-id-1")
                .containsEntry("leader", false)
                .containsEntry("latchPath", "/the/true/path")
                .containsEntry("state", "STARTED")
                .containsKey("participants");

        @SuppressWarnings("unchecked")
        var participants = (List<Map<String, Object>>) entity.get("participants");
        assertThat(participants)
                .hasSize(3)
                .contains(Map.of("id", "test-id-1", "leader", false))
                .contains(Map.of("id", "test-id-2", "leader", false))
                .contains(Map.of("id", "test-id-3", "leader", true));
    }

    private static Collection<Participant> sampleParticipants() {
        return List.of(
                sampleParticipant("test-id-1", false),
                sampleParticipant("test-id-2", false),
                sampleParticipant("test-id-3", true)
        );
    }

    private static Participant sampleParticipant(String id, boolean isLeader) {
        return new Participant(id, isLeader);
    }
}
