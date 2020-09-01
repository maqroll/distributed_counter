package test;

import static test.AeronNode.CLIENT_FACING_PORT_OFFSET;
import static test.AeronNode.calculatePort;
import static test.DistributedCounter.RND_MESSAGE_LENGTH;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

public class AeronClient implements EgressListener {
  private final MutableDirectBuffer actionBidBuffer = new ExpandableDirectByteBuffer();
  private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
  private final Map<Long, Long> counters = new HashMap<>();

  @Override
  public void onMessage(
      long clusterSessionId,
      long timestamp,
      DirectBuffer buffer,
      int offset,
      int length,
      Header header) {
    final long id = buffer.getLong(offset);
    final long rnd = buffer.getLong(offset + BitUtil.SIZE_OF_LONG);

    if (rnd != 0L) {
      counters.put(id, rnd);
    } else {
      counters.remove(id);
    }

    System.out.println("Total -> " + counters);
  }

  @Override
  public void onSessionEvent(
      long correlationId,
      long clusterSessionId,
      long leadershipTermId,
      int leaderMemberId,
      EventCode code,
      String detail) {
    printOutput(
        "SessionEvent("
            + correlationId
            + ", "
            + leadershipTermId
            + ", "
            + leaderMemberId
            + ", "
            + code
            + ", "
            + detail
            + ")");
  }

  @Override
  public void onNewLeader(
      long clusterSessionId, long leadershipTermId, int leaderMemberId, String ingressEndpoints) {
    printOutput(
        "NewLeader(" + clusterSessionId + ", " + leadershipTermId + ", " + leaderMemberId + ")");
  }

  public static String ingressEndpoints(final List<String> hostnames) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < hostnames.size(); i++) {
      sb.append(i).append('=');
      sb.append(hostnames.get(i)).append(':').append(calculatePort(i, CLIENT_FACING_PORT_OFFSET));
      sb.append(',');
    }

    sb.setLength(sb.length() - 1);

    return sb.toString();
  }

  private void sendRandomMessages(final long id, final AeronCluster aeronCluster) {
    while (!Thread.currentThread().isInterrupted()) {
      long nextRnd = System.currentTimeMillis() + ThreadLocalRandom.current().nextLong(100000L);
      send(id, aeronCluster, nextRnd);

      idleStrategy.idle(aeronCluster.pollEgress());
    }
  }

  private void send(final long id, final AeronCluster aeronCluster, final long rnd) {
    actionBidBuffer.putLong(0, id);
    actionBidBuffer.putLong(BitUtil.SIZE_OF_LONG, rnd);

    while (aeronCluster.offer(actionBidBuffer, 0, RND_MESSAGE_LENGTH) < 0) // <2>
    {
      idleStrategy.idle(aeronCluster.pollEgress()); // <3>
    }
  }

  private void printOutput(final String message) {
    System.out.println(message);
  }

  public static void main(final String[] args) {
    // TODO: use id to identify client
    final long id = ThreadLocalRandom.current().nextLong();

    String localName = null;
    try {
      localName = InetAddress.getLocalHost().getHostName();
      System.out.println(localName);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    final String ingressEndpoints = ingressEndpoints(Arrays.asList("raft-1", "raft-2", "raft-3"));
    final AeronClient client = new AeronClient();

    final int egressPort = 19000;

    try (MediaDriver mediaDriver =
            MediaDriver.launchEmbedded(
                new MediaDriver.Context() // <1>
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
        AeronCluster aeronCluster =
            AeronCluster.connect(
                new AeronCluster.Context()
                    .egressListener(client) // <2>
                    // multicast
                    // .egressChannel("aeron:udp?endpoint=224.0.1.1:" + egressPort) // <3>
                    .egressChannel("aeron:udp?endpoint=" + localName + ":" + egressPort) // <3>
                    .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                    .ingressChannel("aeron:udp") // <4>
                    .ingressEndpoints(ingressEndpoints))) {
      while (true) {
        client.sendRandomMessages(id, aeronCluster);
      }
    }
  }
}
