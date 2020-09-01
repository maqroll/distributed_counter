package test;

import static java.lang.Integer.parseInt;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;

public class AeronNode {
  private static final int PORT_BASE = 9000;
  private static final int PORTS_PER_NODE = 100;
  private static final int ARCHIVE_CONTROL_REQUEST_PORT_OFFSET = 1;
  private static final int ARCHIVE_CONTROL_RESPONSE_PORT_OFFSET = 2;
  public static final int CLIENT_FACING_PORT_OFFSET = 3;
  private static final int MEMBER_FACING_PORT_OFFSET = 4;
  private static final int LOG_PORT_OFFSET = 5;
  private static final int TRANSFER_PORT_OFFSET = 6;
  private static final int LOG_CONTROL_PORT_OFFSET = 7;

  public static String member(String localName) {
    StringBuilder sb = new StringBuilder();

    sb.append(localName).append(":").append(calculatePort(0, 3)).append(",");
    sb.append(localName).append(":").append(calculatePort(0, 4)).append(",");
    sb.append(localName).append(":").append(calculatePort(0, 5)).append(",");
    sb.append(localName).append(":").append(calculatePort(0, 6)).append(",");
    sb.append(localName).append(":").append(calculatePort(0, 1));

    System.out.println(sb.toString());
    return sb.toString();
  }

  public static String clusterMembers(List<String> hostnames) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < hostnames.size(); ++i) {
      sb.append(i);
      sb.append(',').append((String) hostnames.get(i)).append(':').append(calculatePort(i, 3));
      sb.append(',').append((String) hostnames.get(i)).append(':').append(calculatePort(i, 4));
      sb.append(',').append((String) hostnames.get(i)).append(':').append(calculatePort(i, 5));
      sb.append(',').append((String) hostnames.get(i)).append(':').append(calculatePort(i, 6));
      sb.append(',').append((String) hostnames.get(i)).append(':').append(calculatePort(i, 1));
      sb.append('|');
    }

    return sb.toString();
  }

  private static String logControlChannel(int nodeId, String hostname, int portOffset) {
    int port = calculatePort(nodeId, portOffset);
    return (new ChannelUriStringBuilder())
        .media("udp")
        .termLength(65536)
        .controlMode("manual")
        .controlEndpoint(hostname + ":" + port)
        .build();
  }

  private static ErrorHandler errorHandler(String context) {
    return (throwable) -> {
      System.err.println(context);
      throwable.printStackTrace(System.err);
    };
  }

  static int calculatePort(final int nodeId, final int offset) {
    return PORT_BASE /*+ (nodeId * PORTS_PER_NODE)*/ + offset;
  }

  private static String udpChannel(final int nodeId, final String hostname, final int portOffset) {
    final int port = calculatePort(nodeId, portOffset);
    return new ChannelUriStringBuilder()
        .media("udp")
        .termLength(64 * 1024)
        .endpoint(hostname + ":" + port)
        .build();
  }

  public static void main(final String[] args) {
    String localName = null;
    try {
      localName = InetAddress.getLocalHost().getHostName();
      System.out.println(localName);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    final int nodeId =
        parseInt(
            localName
                .split("-")[1]); /*parseInt(System.getProperty("aeron.cluster.tutorial.nodeId"));*/

    List<String> hostnames = Arrays.asList("raft-1", "raft-2", "raft-3");
    String hostname = /*(String) hostnames.get(nodeId-1)*/ localName;

    final File baseDir = new File(System.getProperty("user.dir"), "node" /*+ nodeId*/);
    final String aeronDirName =
        CommonContext.getAeronDirectoryName() /*+ "-" + nodeId*/ + "-driver";

    final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

    final MediaDriver.Context mediaDriverContext =
        new MediaDriver.Context()
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
            .terminationHook(barrier::signal)
            .errorHandler(AeronNode.errorHandler("Media Driver"));

    final Archive.Context archiveContext =
        new Archive.Context()
            .archiveDir(new File(baseDir, "archive"))
            .controlChannel(udpChannel(nodeId, localName, ARCHIVE_CONTROL_REQUEST_PORT_OFFSET))
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);

    // client to archive
    final AeronArchive.Context aeronArchiveContext =
        new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveContext.controlChannel())
            .controlRequestStreamId(archiveContext.controlStreamId())
            .controlResponseChannel(
                udpChannel(nodeId, localName, ARCHIVE_CONTROL_RESPONSE_PORT_OFFSET))
            .aeronDirectoryName(aeronDirName);

    final ConsensusModule.Context consensusModuleContext =
        new ConsensusModule.Context()
            .errorHandler(errorHandler("Consensus Module"))
            .clusterMemberId(/*NULL_VALUE*/ nodeId - 1)
            .clusterMembers(
                /*"")
                //   */ clusterMembers(
                    hostnames)) // TODO is it necessary to declare beforehand the
            // .clusterConsensusEndpoints("raft-1:9004,raft-2:9004,raft-3:9004")
            // .memberEndpoints(member(localName))
            .clusterDir(new File(baseDir, "consensus-module"))
            .ingressChannel("aeron:udp?term-length=64k")
            .logChannel(logControlChannel(nodeId, hostname, LOG_CONTROL_PORT_OFFSET))
            .archiveContext(aeronArchiveContext.clone());

    final ClusteredServiceContainer.Context clusteredServiceContext =
        new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(baseDir, "service"))
            .clusteredService(new DistributedCounter("s"))
            .errorHandler(errorHandler("Clustered Service"));

    try (ClusteredMediaDriver clusteredMediaDriver =
            ClusteredMediaDriver.launch(
                mediaDriverContext, archiveContext, consensusModuleContext); // (1)
        ClusteredServiceContainer container =
            ClusteredServiceContainer.launch(clusteredServiceContext)) // (2)
    {
      System.out.println("[" + nodeId + "] Started Cluster Node on " + hostname + "...");
      barrier.await(); // (3)
      System.out.println("[" + nodeId + "] Exiting");
    }
  }
}
