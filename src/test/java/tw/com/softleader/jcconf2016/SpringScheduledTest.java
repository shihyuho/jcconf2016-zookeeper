package tw.com.softleader.jcconf2016;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

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

public class SpringScheduledTest {

  private static String connectString = "localhost:2181";
  private static String rootPath = "/jcconf2016";
  private static int numberOfPrticipants = 5;
  private static AtomicInteger id = new AtomicInteger();

  @Configuration
  @EnableAsync
  @EnableScheduling
  @ComponentScan(basePackages = {"tw.com.softleader.*"})
  public static class ScheduledConfig {

    @Bean(initMethod = "start", destroyMethod = "close")
    public ZooKeeperClient zooKeeperLeaderLatch() {
      return new ZooKeeperClient(connectString, rootPath, "" + id.incrementAndGet());
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
  public void test() throws InterruptedException {

    Collection<ConfigurableApplicationContext> contexts = IntStream.range(0, numberOfPrticipants)
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

}
