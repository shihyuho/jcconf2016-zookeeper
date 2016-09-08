package tw.com.softleader.jcconf2016;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SomeTask {

  @Autowired
  private ZooKeeperClient client;

  @Scheduled(fixedDelay = 500)
  public void printCurrentTime() {
    log.info("{} printed by [{}]", LocalDateTime.now(), client.getParticipantId());
  }

}
