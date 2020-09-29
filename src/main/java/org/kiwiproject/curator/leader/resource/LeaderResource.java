package org.kiwiproject.curator.leader.resource;

import org.kiwiproject.curator.leader.ManagedLeaderLatch;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * JAX-RS resource providing endpoints for checking leadership and participant information.
 */
@Path("/kiwi/leader-latch")
@Produces(MediaType.APPLICATION_JSON)
public class LeaderResource {

    private final ManagedLeaderLatch leaderLatch;

    public LeaderResource(ManagedLeaderLatch leaderLatch) {
        this.leaderLatch = leaderLatch;
    }

    /**
     * Checks whether this service is the latch leader.
     *
     * @return the JSON {@link Response}
     */
    @GET
    @Path("/leader")
    public Response hasLeadership() {
        var entity = Map.of(
                "leader", leaderLatch.hasLeadership()
        );
        return Response.ok(entity).build();
    }

    /**
     * Get information about the leader latch that this service participates in.
     *
     * @return the JSON {@link Response}
     */
    @GET
    @Path("/latch")
    public Response getLatchState() {
        var entity = Map.of(
                "id", leaderLatch.getId(),
                "leader", leaderLatch.hasLeadership(),
                "latchPath", leaderLatch.getLatchPath(),
                "participants", leaderLatch.getParticipants(),
                "state", leaderLatch.getLatchState()
        );
        return Response.ok(entity).build();
    }
}
