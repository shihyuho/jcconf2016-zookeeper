package tw.com.softleader.jcconf2016;

import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ZooKeeperClient implements Watcher, AutoCloseable {

  private final String connectString;
  private final String rootPath;
  private @Getter final String participantId;
  private final AtomicBoolean leader = new AtomicBoolean();
  private ZooKeeper zk;
  private String sequence;

  public void close() {
    if (zk != null) {
      try {
        zk.close();
        log.debug("[{}] ZooKeeper client is closed", participantId);
      } catch (InterruptedException e) {
        log.error("InterruptedException thrown while closing ZooKeeper.", e);
      }
    }
  }

  public void start() {
    try {
      zk = new ZooKeeper(connectString, 15000, this);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    createParent();
    createParticipantAsync();
  }

  private void createParent() {
    String path = rootPath;
    try {
      path = zk.create(rootPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (ConnectionLossException e) {
      createParent();
    } catch (NodeExistsException e) {
      log.debug("[{}] Parent already registered: '{}'", participantId, path);
    } catch (KeeperException | InterruptedException e) {
      log.error("[{}] Something went wrong when creating parent: '{}'", participantId, rootPath, e);
      throw new Error(e);
    }
  }

  private void createParticipantAsync() {
    zk.create(rootPath + "/zookeeper-demo-", participantId.getBytes(StandardCharsets.UTF_8),
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, acquireParticipantSequence, null);
  }

  private StringCallback acquireParticipantSequence = new StringCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      switch (Code.get(rc)) {
        case SESSIONEXPIRED:
        case CONNECTIONLOSS:
          leader.set(false);
          createParticipantAsync();
          break;
        case OK:
          sequence = name.replaceFirst(rootPath + "/", "");
          log.debug("[{}] Participant '{}' created", participantId, sequence);
          checkLeader();
          break;
        case NODEEXISTS:
          log.debug("[{}] Participant '{}' already registered", participantId, name);
          checkLeader();
          break;
        default:
          log.error("[{}] Something went wrong when acquiring Participant sequence for '{}'",
              participantId, rootPath, KeeperException.create(Code.get(rc), path));
          leader.set(false);
      }
    }
  };

  private void checkLeader() {
    zk.getChildren(rootPath, false, attemptToTakeLeadership, null);
  }

  private Watcher znodeDeleted = new Watcher() {
    @Override
    public void process(WatchedEvent e) {
      switch (e.getType()) {
        case NodeDeleted:
          checkLeader();
          break;
        default:
          break;
      }
    }
  };

  private ChildrenCallback attemptToTakeLeadership = new ChildrenCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      switch (Code.get(rc)) {
        case SESSIONEXPIRED:
        case CONNECTIONLOSS:
          leader.set(false);
          checkLeader();
          break;
        case OK:
          Collections.sort(children);
          int index = children.indexOf(sequence);
          if (index == -1) { // Perhaps someone delete znode directly on zookeeper server
            createParticipantAsync();
          } else if (index == 0) {
            if (leader.compareAndSet(false, true)) {
              log.info("[{}] Acquired the leadership", participantId);
            }
          } else {
            if (leader.compareAndSet(true, false)) {
              log.info("[{}] Released the leadership", participantId);
            }
            try {
              zk.getChildren(rootPath + "/" + children.get(index - 1), znodeDeleted);
            } catch (KeeperException | InterruptedException e) {
              checkLeader();
            }
          }
          break;
        default:
          log.error("[{}] Something went wrong when attempting to take leadership", participantId,
              KeeperException.create(Code.get(rc), path));
          leader.set(false);
          checkLeader();
      }
    }
  };

  public void relinquishLeadership(boolean restartAfterwards) {
    try {
      zk.delete(rootPath + "/" + sequence, -1);
      leader.set(false);
      log.debug("[{}] Relinquished leadership", participantId);
    } catch (InterruptedException | KeeperException e) {
      throw new Error(e);
    }
    if (restartAfterwards) {
      createParticipantAsync();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    switch (event.getState()) {
      case AuthFailed:
      case Disconnected:
        checkLeader();
      case Expired:
        close();
        start();
      default:
        break;
    }
  }

  public boolean hasLeadership() {
    return leader.get();
  }

  /**
   * Get all participants, key: childId, value: has leadership
   * 
   * @return
   */
  public Map<String, Boolean> getParticipants() {
    try {
      return zk.getChildren(rootPath, false).stream()
          .collect(toMap(Function.identity(), child -> child.equals(sequence)));
    } catch (KeeperException | InterruptedException e) {
      throw new Error(e);
    }
  }

}
