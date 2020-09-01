package test;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;

/**
 * MINIMAL distributed counter based on jgroups. Partial values are indexed by address (jgroups).
 * Topology changes get rid of partial results contributed from missing nodes. It doesn't enforce
 * reliability on the channel. Relies instead on values regularly updated from sources. Useful to
 * aggregate current users (i.e.) from disposables nodes behind a balancer.
 */
public class DistributedCounter implements ClusteredService {
  // private static final Logger LOGGER = LoggerFactory.getLogger(DistributedCounter.class);
  private final String name;
  private final Map<Long, Long> counters;
  public static final int RND_MESSAGE_LENGTH = 2 * BitUtil.SIZE_OF_LONG;

  private Cluster cluster;
  private IdleStrategy idleStrategy;

  private final MutableDirectBuffer egressMessageBuffer = new ExpandableDirectByteBuffer(4);
  private final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer(4);
  private final MutableDirectBuffer newSessionBuffer = new ExpandableDirectByteBuffer(4);

  private final Map<Long, Long> ids = new HashMap<>();

  private long maxRnd = 0;

  public DistributedCounter(String name) {
    this.counters = new ConcurrentHashMap<>();
    this.name = name;
  }

  @Override
  public void onStart(Cluster cluster, Image snapshot) {
    this.cluster = cluster;
    this.idleStrategy = cluster.idleStrategy();

    System.out.println("started cluster");
    // Discard snapshot
  }

  @Override
  public void onSessionOpen(ClientSession clientSession, long l) {
    System.out.println("New session " + clientSession);
    if (cluster.role() == Cluster.Role.LEADER) {
      // sending state to new node
      for (Map.Entry<Long, Long> c : counters.entrySet()) {
        newSessionBuffer.putLong(0, c.getKey());
        newSessionBuffer.putLong(BitUtil.SIZE_OF_LONG, c.getValue());
        while (clientSession.offer(newSessionBuffer, 0, 2 * BitUtil.SIZE_OF_LONG) < 0) {
          idleStrategy.idle();
        }
      }
    }
  }

  @Override
  public void onSessionClose(ClientSession clientSession, long l, CloseReason closeReason) {
    System.out.println("Closed session " + clientSession);
    if ((cluster.role() == Cluster.Role.LEADER) && ids.containsKey(clientSession.id())) {
      System.out.println(
          "Cleaning counter for session "
              + clientSession.id()
              + " with counter id "
              + ids.get(clientSession.id()));
      buffer.putLong(0, ids.get(clientSession.id()));
      buffer.putLong(BitUtil.SIZE_OF_LONG, 0L);
      while (cluster.offer(buffer, 0, 2 * BitUtil.SIZE_OF_LONG) < 0) {
        idleStrategy.idle();
      }
    }
    ids.remove(clientSession.id());
  }

  @Override
  public void onSessionMessage(
      ClientSession clientSession,
      long l,
      DirectBuffer buffer,
      int offset,
      int length,
      Header header) {
    final long id = buffer.getLong(offset + 0);
    final long rnd = buffer.getLong(offset + BitUtil.SIZE_OF_LONG);

    if (rnd > maxRnd) { // hack to purge part of the messages
      maxRnd = rnd;
      counters.put(id, maxRnd);
      if ((null != clientSession) && (cluster.role() == Cluster.Role.LEADER)) {
        ids.put(clientSession.id(), id);
        egressMessageBuffer.putLong(0, id);
        egressMessageBuffer.putLong(BitUtil.SIZE_OF_LONG, rnd);

        // You can use multicast instead
        cluster.forEachClientSession(
            sess -> {
              while (sess.offer(egressMessageBuffer, 0, 2 * BitUtil.SIZE_OF_LONG) < 0) {
                idleStrategy.idle();
              }
            });
      }
      System.out.println("counters:" + counters); // dump on update
    } else if (rnd == 0) {
      counters.remove(id);
      System.out.println("counters:" + counters); // dump on update
      if (cluster.role() == Cluster.Role.LEADER) {
        egressMessageBuffer.putLong(0, id);
        egressMessageBuffer.putLong(BitUtil.SIZE_OF_LONG, 0L);

        cluster.forEachClientSession(
            sess -> {
              while (sess.offer(egressMessageBuffer, 0, 2 * BitUtil.SIZE_OF_LONG) < 0) {
                idleStrategy.idle();
              }
            });
      }
    }
  }

  @Override
  public void onTimerEvent(long l, long l1) {}

  @Override
  public void onTakeSnapshot(ExclusivePublication exclusivePublication) {
    System.out.println("Discarded taking snapshots");
  }

  @Override
  public void onRoleChange(Cluster.Role role) {
    System.err.println("Changed role to " + role);
  }

  @Override
  public void onTerminate(Cluster cluster) {}
}
