package org.kiwiproject.curator.leader.util;

import static java.util.Objects.nonNull;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.kiwiproject.retry.SimpleRetryer;
import org.slf4j.event.Level;

import java.util.concurrent.TimeUnit;

@UtilityClass
@Slf4j
public class CuratorTestHelpers {

    public static void deleteRecursivelyIfExists(CuratorFramework client, String path) throws Exception {
        if (pathExists(client, path)) {
            deleteRecursively(client, path);
        }
    }

    public static void deleteRecursively(CuratorFramework client, String path) {
        // In GitHub we have seen various intermittent test failures caused by NodeExistsException.
        // See issue: https://github.com/kiwiproject/dropwizard-leader-latch/issues/36

        // The following attempts to delete a path and any children. If the path still exists after
        // attempting to delete it, try again up to a max of 5 attempts.

        var retryer = SimpleRetryer.builder()
                .commonType("delete path: " + path)
                .retryDelayTime(1)
                .retryDelayUnit(TimeUnit.SECONDS)
                .maxAttempts(5)
                .logLevelForSubsequentAttempts(Level.WARN)
                .build();

        // NOTE: returning null forces a retry unless max attempts have been reached
        retryer.tryGetObject(() -> {
            try {
                deletePathAndChildren(client, path);

                return pathExists(client, path) ? null : path;
            } catch (Exception e) {
                LOG.warn("Error deleting path recursively: {}", path, e);
                return null;
            }
        });
    }

    private static void deletePathAndChildren(CuratorFramework client, String path) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public static boolean pathExists(CuratorFramework client, String path) throws Exception {
        var stat = client.checkExists().forPath(path);

        return nonNull(stat);
    }
}
