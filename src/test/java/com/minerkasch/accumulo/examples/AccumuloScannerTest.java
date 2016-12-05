package com.minerkasch.accumulo.examples;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;


public class AccumuloScannerTest extends AccumuloTestBase {

  // Set the BatchWriter configurations
  private static long memBuf = 1000000L; // bytes to store before sending a batch
  private static long timeout = 1000L; // Milliseconds to wait before sending
  private static int numThreads = 10; // Threads to use to write


  @Before
  public void createTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
    Instance instance = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector conn = instance.getConnector(USER, new PasswordToken(PASSWORD));

    if (conn.tableOperations().exists(TABLE)) {
      conn.tableOperations().delete(TABLE);
    }

    conn.tableOperations().create(TABLE);

    BatchWriterConfig writerConfig = new BatchWriterConfig();
    writerConfig.setTimeout(timeout, TimeUnit.MILLISECONDS);
    writerConfig.setMaxMemory(memBuf);
    writerConfig.setMaxWriteThreads(numThreads);

    BatchWriter writer = conn.createBatchWriter(TABLE, writerConfig);

    Mutation m = new Mutation("1");
    m.put("ColFam".getBytes(), "ColQual".getBytes(), "value".getBytes());
    writer.addMutation(m);

    writer.close();
  }


  @Test
  public void ScannerTest() throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));

    AccumuloScanner scanner = new AccumuloScanner();
    String[] args = {"-t", TABLE,
            "-i", mac.getInstanceName(),
            "-u", USER,
            "-p", PASSWORD,
            "-z", mac.getZooKeepers()};
    scanner.main(args);

    assertEquals("1\tColFam:ColQual\tvalue\n", outContent.toString());
    System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
  }

}
