package org.kiwiproject.curator.leader.resource;

import static org.kiwiproject.test.jaxrs.JaxrsTestHelper.assertNoContentResponse;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
@DisplayName("GotLeaderLatchResource")
class GotLeaderLatchResourceTest {

    private static final ResourceExtension RESOURCE = ResourceExtension.builder()
            .bootstrapLogging(false)
            .addResource(new GotLeaderLatchResource())
            .build();

    @Test
    void shouldReturn_204_NoContent() {
        var response = RESOURCE.client().target("/kiwi/got-leader-latch?").request().get();
        assertNoContentResponse(response);
    }
}
