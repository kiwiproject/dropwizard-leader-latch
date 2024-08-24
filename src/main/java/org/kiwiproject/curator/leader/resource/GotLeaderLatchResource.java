package org.kiwiproject.curator.leader.resource;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * JAX-RS resource providing a single endpoint that allows clients to determine whether this service
 * participates in a leader latch.
 * <p>
 * Simply make a {@code GET /kiwi/got-leader-latch?} request. The question mark is not required but
 * is perhaps a little more fun depending on your sense of humor, i.e., if you think programmer humor is funny
 * or terrible.
 * <p>
 * This resource should, of course, only be registered when the service participates in a leader latch.
 */
@Path("/kiwi/got-leader-latch")
@Produces(MediaType.APPLICATION_JSON)
public class GotLeaderLatchResource {

    /**
     * Returns a 204 (No Content) response if available. Otherwise, a 404 is returned (though not
     * by this resource).
     *
     * @return the response
     */
    @GET
    public Response gotLeaderLatch() {
        return Response.noContent().build();
    }
}
