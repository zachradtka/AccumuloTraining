package com.minerkasch.accumulo.examples;

import com.google.common.io.Files;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

public class AccumuloTestBase {

  protected static final String TABLE = "testTable";
  protected static final String PASSWORD = "password";
  protected static final String USER = "root";
  protected static MiniAccumuloCluster mac;

  @BeforeClass
  public static void setup() throws AccumuloSecurityException, AccumuloException, IOException, InterruptedException {


    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();

    mac = new MiniAccumuloCluster(tempDir, PASSWORD);
    mac.start();

    System.out.println("Minicluster Starting...");
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    System.out.println("Shutting down MAC");
    mac.stop();
  }
}
