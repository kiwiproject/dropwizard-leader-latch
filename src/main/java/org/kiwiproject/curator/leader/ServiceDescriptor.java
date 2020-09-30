package org.kiwiproject.curator.leader;

import lombok.Builder;
import lombok.Value;

/**
 * Value class that contains metadata about a service that participates in a leader latch.
 */
@Value
@Builder
public class ServiceDescriptor {
    String name;
    String version;
    String hostname;
    int port;
}
