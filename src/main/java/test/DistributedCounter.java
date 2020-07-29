package test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jgroups.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MINIMAL distributed counter based on jgroups. Partial values are indexed by address (jgroups).
 * Topology changes get rid of partial results contributed from missing nodes. It doesn't enforce
 * reliability on the channel. Relies instead on values regularly updated from sources. Useful to
 * aggregate current users (i.e.) from disposables nodes behind a balancer.
 */
public class DistributedCounter implements Receiver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedCounter.class);
  private JChannel channel;
  private final String name;
  private final Map<Address, Integer> counters;

  public DistributedCounter(String name) {
    this.counters = new ConcurrentHashMap<>();
    this.name = name;
  }

  public void start() throws Exception {
    channel = new JChannel();
    channel.setDiscardOwnMessages(true);
    channel.setReceiver(this);
    channel.connect(name);
    LOGGER.debug("Started distributed counter \"{}\" at {}", name, channel.address());
  }

  public void stop() {
    channel.close();
  }

  public void push(Integer newValue) throws Exception {
    counters.put(channel.address(), newValue);
    Message msg = new Message(null, newValue);
    channel.send(msg);
    LOGGER.debug("Pushing {} from {} on counter \"{}\"", newValue, channel.address(), name);
  }

  public int total() {
    return counters.values().stream().reduce(0, Integer::sum);
  }

  @Override
  public void receive(Message msg) {
    LOGGER.debug("Received update from {} on counter \"{}\"", msg.getSrc(), name);
    counters.put(msg.getSrc(), (Integer) msg.getObject());
  }

  @Override
  public void viewAccepted(View view) {
    LOGGER.debug("New view on counter \"{}\": ", name, view.toString());
    counters.keySet().retainAll(view.getMembers());
  }
}
