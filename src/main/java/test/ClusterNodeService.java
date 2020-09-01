package test;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;

// Developer supplied application logic.
// Mainly reacting to messages.
public class ClusterNodeService implements ClusteredService {
  private Cluster cluster;
  private IdleStrategy idleStrategy;

  @Override
  // snapshotImage represents LAST snapshot image.
  // If necessary cluster will replay messages on you.
  public void onStart(final Cluster cluster, final Image snapshotImage) {
    this.cluster = cluster; // Store a reference to the cluster
    this.idleStrategy = cluster.idleStrategy(); // Store a reference to the idle strategy

    // snapshotImage represents agreed log
    // discarded
  }

  @Override
  public void onSessionOpen(ClientSession clientSession, long l) {
    // Intentionally left blank
  }

  @Override
  public void onSessionClose(ClientSession clientSession, long l, CloseReason closeReason) {
    // Intentionally left blank
    // TODO enviar al cluster un mensaje indicando el nodo
    // que se pone a cero.
    // ¿Cómo sabemos cual es el nodo? ¿Tiene una descripción?
  }

  @Override
  // Messages reach here by being published to Cluster’s ingress channel.
  // This method will also be passed messages during LOG REPLAY.
  public void onSessionMessage(
      ClientSession clientSession,
      long timestamp,
      DirectBuffer directBuffer,
      int offset,
      int length,
      Header header) {
    if (clientSession == null) {
      System.out.println("Msg WITHOUT session at " + timestamp);
    } else {
      System.out.println("Msg WITH session at " + timestamp);
    }
  }

  @Override
  public void onTimerEvent(long l, long l1) {
    // Intentionally left blank
  }

  @Override
  public void onTakeSnapshot(ExclusivePublication exclusivePublication) {
    System.out.println("Write state");
  }

  @Override
  public void onRoleChange(Cluster.Role role) {
    // Intentionally left blank
    System.out.println("Role change");
  }

  @Override
  public void onTerminate(Cluster cluster) {
    // Intentionally left blank
  }
}
