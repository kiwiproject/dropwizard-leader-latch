### Dropwizard Leader Latch

TODO Project badges...

This is a small library that integrates Apache Curator's Leader Latch recipe
into a Dropwizard service.

This is useful when you are running multiple instances of the same Dropwizard
service, but there are some actions that should only be taken by one of those
instances. Using this library, each group of related Dropwizard service instances
will have exactly one leader, and each instance is able to easily determine if
it is the leader or not.

