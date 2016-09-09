package tw.com.softleader.jcconf2016;

import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class ZooKeeperClientTest {

  private static String connectString;
  private static String rootPath;
  private static int numberOfParticipants;

  @BeforeClass
  public static void setUp() {
    connectString =
        Optional.ofNullable(System.getProperty("connectString")).orElse("localhost:2181");
    rootPath = Optional.ofNullable(System.getProperty("rootPath")).orElse("/jcconf2016");
    numberOfParticipants = Optional.ofNullable(System.getProperty("numberOfParticipants"))
        .map(Integer::parseInt).orElse(5);

    if (numberOfParticipants < 2) {
      Assert.fail("The number of participants must >= 2, but was " + numberOfParticipants);
    }
  }

  @Test
  public void testClient() throws InterruptedException {
    Collection<ZooKeeperClient> participants = IntStream.range(0, numberOfParticipants)
        .mapToObj(i -> new ZooKeeperClient(connectString, rootPath, "" + i)).collect(toList());

    participants.forEach(ZooKeeperClient::start);
    TimeUnit.SECONDS.sleep(1); // just a short wait for all participants connecting to server

    try {
      testAcquireAndRelinquishLeadership(participants);
    } finally {
      participants.forEach(ZooKeeperClient::close);
    }
  }

  @Configuration
  @EnableAsync
  @EnableScheduling
  @ComponentScan(basePackages = {"tw.com.softleader.*"})
  public static class ScheduledConfig {

    private static AtomicInteger id = new AtomicInteger();

    @Bean(initMethod = "start", destroyMethod = "close")
    public ZooKeeperClient zooKeeperLeaderLatch() {
      return new ZooKeeperClient(connectString, rootPath, "" + id.getAndIncrement());
    }

    @Bean
    @Primary
    public TaskScheduler taskScheduler(ZooKeeperClient zooKeeperLeaderLatch) {
      ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
      taskScheduler.afterPropertiesSet();
      return new ZookeeperTaskScheduler(zooKeeperLeaderLatch, taskScheduler);
    }
  }

  @Test
  public void testSpringTask() throws InterruptedException {
    Collection<ConfigurableApplicationContext> contexts = IntStream.range(0, numberOfParticipants)
        .mapToObj(i -> new AnnotationConfigApplicationContext(ScheduledConfig.class))
        .collect(toList());
    TimeUnit.SECONDS.sleep(1); // just a short wait for all participants connecting to server

    try {
      Collection<ZooKeeperClient> participants =
          contexts.stream().map(ctx -> ctx.getBean(ZooKeeperClient.class)).collect(toList());
      new ZooKeeperClientTest().testAcquireAndRelinquishLeadership(participants);
    } finally {
      contexts.forEach(ConfigurableApplicationContext::close);
    }

  }


  private void testAcquireAndRelinquishLeadership(Collection<ZooKeeperClient> participants)
      throws InterruptedException {
    Map<Boolean, List<ZooKeeperClient>> leaderships =
        participants.stream().collect(partitioningBy(ZooKeeperClient::hasLeadership));

    List<ZooKeeperClient> ownLeaderships = leaderships.get(true);
    Assert.assertEquals(1, ownLeaderships.size());
    ZooKeeperClient leader = ownLeaderships.get(0);

    List<ZooKeeperClient> followers = leaderships.get(false);
    Assert.assertEquals(numberOfParticipants - 1, followers.size());
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
