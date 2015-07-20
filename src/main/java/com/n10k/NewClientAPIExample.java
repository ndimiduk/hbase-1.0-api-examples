/*
 * Copyright 2014 Nick Dimiduk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.n10k;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class NewClientAPIExample extends Configured implements Tool {

  /** The identifier for the application table. */
  private static final TableName TABLE_NAME = TableName.valueOf("MyTable");
  /** The name of the column family used by the application. */
  private static final byte[] CF = Bytes.toBytes("cf1");

  public int run(String[] argv) throws IOException {
    setConf(HBaseConfiguration.create(getConf()));

    /** Connection to the cluster. A single connection shared by all application threads. */
    Connection connection = null;
    /** A lightweight handle to a specific table. Used from a single thread. */
    Table table = null;
    try {
      // establish the connection to the cluster.
      connection = ConnectionFactory.createConnection(getConf());
      // retrieve a handle to the target table.
      table = connection.getTable(TABLE_NAME);
      // describe the data we want to write.
      Put p = new Put(Bytes.toBytes("someRow"));
      p.add(CF, Bytes.toBytes("qual"), Bytes.toBytes(42.0d));
      // send the data.
      table.put(p);
    } finally {
      // close everything down
      if (table != null) table.close();
      if (connection != null) connection.close();
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new NewClientAPIExample(), argv);
    System.exit(ret);
  }
}
