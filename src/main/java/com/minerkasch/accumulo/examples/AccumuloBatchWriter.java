package com.minerkasch.accumulo.examples;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import java.util.concurrent.TimeUnit;

public class AccumuloBatchWriter {

  public static void main(String[] args) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {

    ClientOnRequiredTable client = new ClientOnRequiredTable();
    client.parseArgs(AccumuloScanner.class.getName(), args);

    Connector conn = client.getConnector();

    // Create Table if it doesn't already exist
    if (!conn.tableOperations().exists(client.getTableName())) {
      conn.tableOperations().create(client.getTableName());
    }

    // Set the BatchWriter configurations
    long memBuf = 1000000L; // bytes to store before sending a batch
    long timeout = 1000L; // Milliseconds to wait before sending
    int numThreads = 10; // Threads to use to write

    BatchWriterConfig writerConfig = new BatchWriterConfig();
    writerConfig.setTimeout(timeout, TimeUnit.MILLISECONDS);
    writerConfig.setMaxMemory(memBuf);
    writerConfig.setMaxWriteThreads(numThreads);

    BatchWriter writer = conn.createBatchWriter(client.getTableName(), writerConfig);

    Mutation m = new Mutation("1");
    m.put("ColFam".getBytes(), "ColQual".getBytes(), "value".getBytes());
    writer.addMutation(m);

    writer.close();
  }
}
