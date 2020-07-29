package test;

public class JGroupNode {
  public static void main(String[] args) throws Exception {
    DistributedCounter counter = new DistributedCounter("active sessions");

    counter.start();

    int i = 0;
    while (true) {
      counter.push((++i) % 100);
      Thread.sleep(500);
    }
  }
}
