package com.minerkasch.accumulo.examples;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class AccumuloBatchWriterTest extends AccumuloTestBase {


  @Before
  public void createTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
    Instance instance = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector conn = instance.getConnector(USER, new PasswordToken(PASSWORD));

    if (conn.tableOperations().exists(TABLE)) {
      conn.tableOperations().delete(TABLE);
    }

    conn.tableOperations().create(TABLE);
  }


  @Test
  public void batchWriterTest() throws AccumuloSecurityException, TableNotFoundException, AccumuloException, TableExistsException {

    AccumuloBatchWriter batchWriter = new AccumuloBatchWriter();
    String[] args = {"-t", TABLE,
            "-i", mac.getInstanceName(),
            "-u", USER,
            "-p", PASSWORD,
            "-z", mac.getZooKeepers()};
    batchWriter.main(args);

    Instance instance = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector conn = instance.getConnector(USER, new PasswordToken(PASSWORD));
    Scanner scan = conn.createScanner(TABLE, new Authorizations());

    for (Map.Entry<Key, Value> record : scan) {
      assertEquals("value", record.getValue().toString());
    }

    scan.close();
  }
}
