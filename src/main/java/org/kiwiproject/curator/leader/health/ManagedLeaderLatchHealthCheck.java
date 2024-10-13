package org.kiwiproject.curator.leader.health;

import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.collect.KiwiLists.isNullOrEmpty;
import static org.kiwiproject.metrics.health.HealthCheckResults.newHealthyResultBuilder;
import static org.kiwiproject.metrics.health.HealthCheckResults.newUnhealthyResultBuilder;
import static org.kiwiproject.metrics.health.HealthStatus.CRITICAL;

import com.codahale.metrics.health.HealthCheck;
import org.apache.curator.framework.recipes.leader.Participant;
import org.kiwiproject.curator.leader.ManagedLeaderLatch;

import java.util.Collection;
import java.util.List;

/**
 * Simple check to determine if the leader latch is started (healthy) or otherwise (not healthy).
 * <p>
 * The check also reports as unhealthy if there is no leader, or if Curator reports more than one leader.
 * <p>
 * The health check results contain the following details:
 * <table>
 *     <caption>Health Check Details</caption>
 *     <tr>
 *         <th>Name</th>
 *         <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td>leader</td>
 *         <td>A boolean that is true if this participant is currently the leader, and false otherwise.</td>
 *     </tr>
 *     <tr>
 *         <td>leaderParticipant</td>
 *         <td>
 *             The ID of the latch participant that is the leader.
 *             This will be null if the latch is not started or if no leader is reported.
 *         </td>
 *     </tr>
 *     <tr>
 *         <td>thisParticipant</td>
 *         <td>
 *             The ID of this participant.
 *         </td>
 *     </tr>
 *     <tr>
 *         <td>participants</td>
 *         <td>
 *             A list containing the IDs of all latch participants.
 *             This will be empty if the latch is not started or if no latch participants are reported.
 *         </td>
 *     </tr>
 * </table>
 */
public class ManagedLeaderLatchHealthCheck extends HealthCheck {

    private static final String LEADER_DETAIL_NAME = "leader";
    private static final String LEADER_PARTICIPANT_DETAIL_NAME = "leaderParticipant";
    private static final String THIS_PARTICIPANT_DETAIL_NAME = "thisParticipant";
    private static final String PARTICIPANTS_DETAIL_NAME = "participants";

    private final ManagedLeaderLatch leaderLatch;

    /**
     * New health check instance for the given leader latch.
     *
     * @param leaderLatch the {@link ManagedLeaderLatch} to check
     */
    public ManagedLeaderLatchHealthCheck(ManagedLeaderLatch leaderLatch) {
        this.leaderLatch = leaderLatch;
    }

    @Override
    protected Result check() {
        var participantId = leaderLatch.getId();

        if (!leaderLatch.isStarted()) {
            return newUnhealthyResultBuilder(CRITICAL)
                    .withMessage("Leader latch state (%s) is not started", leaderLatch.getLatchState())
                    .withDetail(LEADER_DETAIL_NAME, false)
                    .withDetail(LEADER_PARTICIPANT_DETAIL_NAME, null)
                    .withDetail(THIS_PARTICIPANT_DETAIL_NAME, participantId)
                    .withDetail(PARTICIPANTS_DETAIL_NAME, List.of())
                    .build();
        }

        var participants = leaderLatch.getParticipants();
        var leaderIds = leaderIdsOf(participants);
        var participantIds = idsOf(participants);

        if (isNullOrEmpty(leaderIds)) {
            return newUnhealthyResultBuilder(CRITICAL)
                    .withMessage("There are NO leaders for latch path %s", leaderLatch.getLatchPath())
                    .withDetail(LEADER_DETAIL_NAME, false)
                    .withDetail(LEADER_PARTICIPANT_DETAIL_NAME, null)
                    .withDetail(THIS_PARTICIPANT_DETAIL_NAME, participantId)
                    .withDetail(PARTICIPANTS_DETAIL_NAME, participantIds)
                    .build();

        } else if (leaderIds.size() > 1) {
            return newUnhealthyResultBuilder(CRITICAL)
                    .withMessage("There is more than one leader for latch path %s. Leader IDs: %s (this latch ID: %s)",
                            leaderLatch.getLatchPath(), leaderIds, leaderLatch.getId())
                    .withDetail(LEADER_DETAIL_NAME, leaderLatch.hasLeadership())
                    .withDetail(LEADER_PARTICIPANT_DETAIL_NAME, first(leaderIds))  // only include the first one here
                    .withDetail(THIS_PARTICIPANT_DETAIL_NAME, participantId)
                    .withDetail(PARTICIPANTS_DETAIL_NAME, participantIds)
                    .build();
        }

        return newHealthyResultBuilder()
                .withMessage("Leader latch is started (has leadership? %s)", leaderLatch.hasLeadership())
                .withDetail(LEADER_DETAIL_NAME, leaderLatch.hasLeadership())
                .withDetail(LEADER_PARTICIPANT_DETAIL_NAME, first(leaderIds))
                .withDetail(THIS_PARTICIPANT_DETAIL_NAME, participantId)
                .withDetail(PARTICIPANTS_DETAIL_NAME, participantIds)
                .build();
    }

    private static List<String> leaderIdsOf(Collection<Participant> participants) {
        return participants.stream()
                .filter(Participant::isLeader)
                .map(Participant::getId)
                .toList();
    }

    private static List<String> idsOf(Collection<Participant> participants) {
        return participants.stream()
                .map(Participant::getId)
                .toList();
    }

}
