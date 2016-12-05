package com.minerkasch.accumulo.examples;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public class AccumuloBatchScanner {

  public static void main(String args[]) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    ClientOnRequiredTable client = new ClientOnRequiredTable();
    ScannerOpts scannerOpts = new ScannerOpts();
    client.parseArgs(BatchScanner.class.getName(), args, scannerOpts);

    Connector conn = client.getConnector();
    BatchScanner bscan = conn.createBatchScanner(client.getTableName(), client.auths, 10);

    for (Map.Entry<Key, Value> record : bscan) {
      String output = String.format("%s\t%s:%s\t%s", record.getKey().getRow().toString(),
              record.getKey().getColumnFamily().toString(), record.getKey().getColumnQualifier().toString(),
              record.getValue().toString());
      System.out.println(output);
    }

    bscan.close();
  }
}
