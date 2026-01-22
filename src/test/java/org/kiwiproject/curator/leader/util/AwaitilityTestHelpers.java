package org.kiwiproject.curator.leader.util;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;

import lombok.experimental.UtilityClass;
import org.awaitility.core.ConditionFactory;
import org.awaitility.core.ThrowingRunnable;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@UtilityClass
public class AwaitilityTestHelpers {

    public static void await5SecondsUntilAsserted(ThrowingRunnable assertion) {
        await5Seconds().untilAsserted(assertion);
    }

    public static void await5SecondsUntilTrue(AtomicBoolean condition) {
        await5Seconds().untilTrue(condition);
    }

    public static void await5SecondsUntilTrue(Callable<Boolean> condition) {
        await5Seconds().until(condition);
    }

    public static <V> void await5SecondsUntilFutureIsDone(Future<V> future) {
        await5Seconds().until(future::isDone);
    }

    public static ConditionFactory await5Seconds() {
        return await().atMost(FIVE_SECONDS);
    }
}
