package org.kiwiproject.curator.leader.test.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import lombok.experimental.UtilityClass;
import org.junit.jupiter.api.DynamicTest;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Utilities to generate {@link DynamicTest}s for testing "standard" exceptions, where "standard" is defined as
 * an exception class that is a subclass of {@link Exception} and contains the same four public constructors that
 * do nothing but delegate to the superclass implementations. Any custom logic cannot be known in advanced, so only
 * this "standard" behavior is tested here.
 */
// TODO Migrate this to kiwi-test
@UtilityClass
public class StandardExceptionTests {

    public static Collection<DynamicTest> standardConstructorTestsFor(Class<? extends Exception> exceptionClass) {
        return List.of(
                noArgConstructorDynamicTest(exceptionClass),
                messageConstructorDynamicTest(exceptionClass),
                messageAndCauseConstructorDynamicTest(exceptionClass),
                causeConstructorDynamicTest(exceptionClass)
        );
    }

    public static DynamicTest noArgConstructorDynamicTest(Class<? extends Exception> exceptionClass) {
        var displayName = displayName(exceptionClass, "()");
        return dynamicTest(displayName, () -> {
            var constructor = exceptionClass.getConstructor();
            var exception = constructor.newInstance();

            assertThat(exception)
                    .isExactlyInstanceOf(exceptionClass)
                    .hasMessage(null)
                    .hasNoCause();
        });
    }

    public static DynamicTest messageConstructorDynamicTest(Class<? extends Exception> exceptionClass) {
        var displayName = displayName(exceptionClass, "(String message)");
        return dynamicTest(displayName, () -> {
            var constructor = exceptionClass.getConstructor(String.class);
            var message = "An error occurred";
            var exception = constructor.newInstance(message);

            assertThat(exception)
                    .isExactlyInstanceOf(exceptionClass)
                    .hasMessage(message)
                    .hasNoCause();
        });
    }

    public static DynamicTest messageAndCauseConstructorDynamicTest(Class<? extends Exception> exceptionClass) {
        var displayName = displayName(exceptionClass, "(String message, Throwable cause)");
        return dynamicTest(displayName, () -> {
            var constructor = exceptionClass.getConstructor(String.class, Throwable.class);
            var message = "An I/O related error occurred";
            var cause = new IOException("I/O error");
            var exception = constructor.newInstance(message, cause);

            assertThat(exception)
                    .isExactlyInstanceOf(exceptionClass)
                    .hasMessage(message)
                    .hasCause(cause);
        });
    }

    public static DynamicTest causeConstructorDynamicTest(Class<? extends Exception> exceptionClass) {
        var displayName = displayName(exceptionClass, "(Throwable cause)");
        return dynamicTest(displayName, () -> {
            var constructor = exceptionClass.getConstructor(Throwable.class);
            var cause = new IOException("An unexpected I/O error occurred");
            var exception = constructor.newInstance(cause);

            assertThat(exception)
                    .isExactlyInstanceOf(exceptionClass)
                    .hasMessageContaining("IOException")
                    .hasMessageContaining("An unexpected I/O error occurred")
                    .hasCause(cause);
        });
    }

    private static String displayName(Class<? extends Exception> exceptionClass, String parameterSpec) {
        return exceptionClass.getSimpleName() + parameterSpec;
    }
}
