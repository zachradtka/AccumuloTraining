package com.minerkasch.accumulo.examples;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;


public class MockAccumulo {
  public static void main(String args[]) {
    Instance inst = new MockInstance();

    System.out.println("Instance Name: " + inst.getInstanceName());
    System.out.println("Zookeepers: " + inst.getZooKeepers());

    while (true);

  }
}
