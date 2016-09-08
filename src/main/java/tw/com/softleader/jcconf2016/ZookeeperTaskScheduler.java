package tw.com.softleader.jcconf2016;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;

import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ZookeeperTaskScheduler implements TaskScheduler {

  private final ZooKeeperClient client;
  private final TaskScheduler delegate;

  public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
    return delegate.schedule(runsIfOwnLeadership(task), trigger);
  }

  public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
    return delegate.schedule(runsIfOwnLeadership(task), startTime);
  }

  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
    return delegate.scheduleAtFixedRate(runsIfOwnLeadership(task), startTime, period);
  }

  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
    return delegate.scheduleAtFixedRate(runsIfOwnLeadership(task), period);
  }

  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
    return delegate.scheduleWithFixedDelay(runsIfOwnLeadership(task), startTime, delay);
  }

  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
    return delegate.scheduleWithFixedDelay(runsIfOwnLeadership(task), delay);
  }

  private Runnable runsIfOwnLeadership(Runnable runnable) {
    return () -> {
      if (client.hasLeadership()) {
        runnable.run();
      }
    };
  }
}
