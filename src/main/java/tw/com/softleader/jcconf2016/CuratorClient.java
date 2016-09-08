package tw.com.softleader.jcconf2016;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CuratorClient implements PathChildrenCacheListener, AutoCloseable {

  private final String connectString;
  private final String rootPath;
  private @Getter final String participantId;
  private LeaderLatch leaderLatch;
  private CuratorFramework client;
  private PathChildrenCache cache;

  public void start() throws Exception {
    client =
        CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 29));
    client.start();
    try {
      client.getZookeeperClient().blockUntilConnectedOrTimedOut();
    } catch (InterruptedException e) {
      client.close();
      start();
    }

    leaderLatch = new LeaderLatch(client, rootPath, participantId);
    leaderLatch.start();

    cache = new PathChildrenCache(client, rootPath, true);
    cache.start();
    cache.getListenable().addListener(this);
  }

  @Override
  public void close() {
    CloseableUtils.closeQuietly(cache);
    CloseableUtils.closeQuietly(leaderLatch);
    CloseableUtils.closeQuietly(client);
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    switch (event.getType()) {
      case CHILD_REMOVED:
        String removedId = new String(event.getData().getData(), StandardCharsets.UTF_8);
        if (removedId.equals(participantId)) {
          close();
          start();
        }
      default:
        break;
    }
  }

  public Collection<Participant> getParticipants() throws Exception {
    return leaderLatch.getParticipants();
  }

  public boolean hasLeadership() {
    return leaderLatch.hasLeadership();
  }

  public void relinquishLeadership(boolean restartAfterwards) throws Exception {
    if (hasLeadership()) {
      try {
        close();
      } catch (Exception e) {
      }
      if (restartAfterwards) {
        start();
      }
    }
  }

}
