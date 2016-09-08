package tw.com.softleader.jcconf2016;

import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import tw.com.softleader.jcconf2016.ZooKeeperClient;

public class ZooKeeperClientTest {

  private static String connectString = "localhost:2181";
  private static String rootPath = "/jcconf2016";
  private static int numberOfPrticipants = 5;

  @Test
  public void test() throws InterruptedException {
    if (numberOfPrticipants < 2) {
      Assert.fail("The number of participants must >= 2, but was " + numberOfPrticipants);
    }

    Collection<ZooKeeperClient> participants = IntStream.range(0, numberOfPrticipants)
        .mapToObj(i -> new ZooKeeperClient(connectString, rootPath, "" + i)).collect(toList());

    participants.forEach(ZooKeeperClient::start);
    TimeUnit.SECONDS.sleep(1); // just a short wait for all participants connecting to server

    try {
      testAcquireAndRelinquishLeadership(participants);
    } finally {
      participants.forEach(ZooKeeperClient::close);
    }
  }

  public void testAcquireAndRelinquishLeadership(Collection<ZooKeeperClient> participants)
      throws InterruptedException {
    Map<Boolean, List<ZooKeeperClient>> leaderships =
        participants.stream().collect(partitioningBy(ZooKeeperClient::hasLeadership));

    List<ZooKeeperClient> ownLeaderships = leaderships.get(true);
    Assert.assertEquals(1, ownLeaderships.size());
    ZooKeeperClient leader = ownLeaderships.get(0);

    List<ZooKeeperClient> followers = leaderships.get(false);
    Assert.assertEquals(numberOfPrticipants - 1, followers.size());
    followers.forEach(
        follower -> Assert.assertNotEquals(leader.getParticipantId(), follower.getParticipantId()));

    leader.relinquishLeadership(true);

    TimeUnit.SECONDS.sleep(1); // another short wait for the followers action to NodeDeleted

    Assert.assertFalse(leader.hasLeadership());

    leaderships = participants.stream().collect(partitioningBy(ZooKeeperClient::hasLeadership));
    ownLeaderships = leaderships.get(true);
    Assert.assertEquals(1, ownLeaderships.size());
    ZooKeeperClient newLeader = ownLeaderships.get(0);

    Assert.assertNotEquals(leader.getParticipantId(), newLeader.getParticipantId());
  }

}
