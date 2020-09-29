package org.kiwiproject.curator.leader;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ServiceDescriptor {
    String name;
    String version;
    String hostname;
    int port;
}
