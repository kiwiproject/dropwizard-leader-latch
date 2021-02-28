package org.kiwiproject.curator.leader.util;

import static java.util.Objects.nonNull;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@UtilityClass
@Slf4j
public class CuratorTestHelpers {

    public static void deleteRecursively(CuratorFramework client, String rootPath) throws Exception {
        if (pathExists(client, rootPath)) {
            LOG.debug("Path {} exists, attempting to delete it", rootPath);
            client.delete().deletingChildrenIfNeeded().forPath(rootPath);

            // In GitHub we have intermittent test failures caused by NodeExistsException thrown in setUp.
            // The following attempts to wait and see if it gets deleted.
            // See issue: https://github.com/kiwiproject/dropwizard-leader-latch/issues/36
            if (pathExists(client, rootPath)) {
                LOG.warn("Path {} still exists; wait up to five seconds for it to be deleted", rootPath);
                await().atMost(FIVE_SECONDS).until(() -> !pathExists(client, rootPath));
            }
        }
    }

    public static boolean pathExists(CuratorFramework client, String rootPath) throws Exception {
        var stat = client.checkExists().forPath(rootPath);

        return nonNull(stat);
    }
}
