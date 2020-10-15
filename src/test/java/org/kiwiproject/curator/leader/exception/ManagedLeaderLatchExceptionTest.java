package org.kiwiproject.curator.leader.exception;

import static org.kiwiproject.test.junit.jupiter.StandardExceptionTests.standardConstructorTestsFor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Collection;

@DisplayName("ManagedLeaderLatchException")
class ManagedLeaderLatchExceptionTest {

    @TestFactory
    Collection<DynamicTest> shouldHaveStandardConstructors() {
        return standardConstructorTestsFor(ManagedLeaderLatchException.class);
    }
}
