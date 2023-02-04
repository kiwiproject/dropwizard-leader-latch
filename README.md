### Dropwizard Leader Latch

[![Build](https://github.com/kiwiproject/dropwizard-leader-latch/workflows/build/badge.svg)](https://github.com/kiwiproject/dropwizard-leader-latch/actions?query=workflow%3Abuild)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_dropwizard-leader-latch&metric=alert_status)](https://sonarcloud.io/dashboard?id=kiwiproject_dropwizard-leader-latch)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_dropwizard-leader-latch&metric=coverage)](https://sonarcloud.io/dashboard?id=kiwiproject_dropwizard-leader-latch)
[![CodeQL](https://github.com/kiwiproject/dropwizard-leader-latch/actions/workflows/codeql.yml/badge.svg)](https://github.com/kiwiproject/dropwizard-leader-latch/actions/workflows/codeql.yml)
[![javadoc](https://javadoc.io/badge2/org.kiwiproject/dropwizard-leader-latch/javadoc.svg)](https://javadoc.io/doc/org.kiwiproject/dropwizard-leader-latch)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Maven Central](https://img.shields.io/maven-central/v/org.kiwiproject/dropwizard-leader-latch)](https://search.maven.org/search?q=g:org.kiwiproject%20a:dropwizard-leader-latch)

This is a small library that integrates Apache Curator's Leader Latch recipe
into a Dropwizard service.

This is useful when you are running multiple instances of the same Dropwizard
service, but there are some actions that should only be taken by one of those
instances. Using this library, each group of related Dropwizard service instances
will have exactly one leader, and each instance is able to easily determine if
it is the leader or not.

