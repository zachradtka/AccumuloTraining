package com.minerkasch.accumulo.examples;


import com.google.common.io.Files;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class MiniAccumulo {

  protected static final String PASSWORD = "password";
  protected static MiniAccumuloCluster mac;

  public static void main(String args[]) throws IOException, InterruptedException {
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();

    mac = new MiniAccumuloCluster(tempDir, PASSWORD);
    mac.start();

    System.out.println("Minicluster Starting...");

    System.out.println("Instnace Name: " + mac.getInstanceName());
    System.out.println("ZooKeepers: " + mac.getZooKeepers());

    Boolean run = true;
    do {
      Thread.sleep(1000);
    } while(run);


    System.out.println("Shutting down MAC");
    mac.stop();
  }
}
