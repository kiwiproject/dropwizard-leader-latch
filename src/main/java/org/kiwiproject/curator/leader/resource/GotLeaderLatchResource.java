package org.kiwiproject.curator.leader.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * JAX-RS resource providing a single endpoint that allows clients to determine whether this service
 * participates in a leader latch.
 * <p>
 * Simply make a {@code GET /kiwi/got-leader-latch?} request. The question mark is clearly not required but
 * is perhaps a little more fun depending on your sense of humor, i.e. if you think programmer humor is funny
 * or terrible.
 * <p>
 * This resource should, of course, only be registered when the service participates in a leader latch.
 */
@Path("/kiwi/got-leader-latch")
@Produces(MediaType.APPLICATION_JSON)
public class GotLeaderLatchResource {

    /**
     * Returns a 204 (No Content) response if available. Otherwise, a 404 is returned (though obviously not
     * by this resource).
     *
     * @return the response
     */
    @GET
    public Response gotLeaderLatch() {
        return Response.noContent().build();
    }
}
