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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class OldClientAPIExample extends Configured implements Tool {

  /** The identifier for the application table. */
  private static final byte[] TABLE_NAME = Bytes.toBytes("MyTable");
  /** The name of the column family used by the application. */
  private static final byte[] CF = Bytes.toBytes("cf1");

  public int run(String[] argv) throws IOException {
    setConf(HBaseConfiguration.create(getConf()));

    // Encapsulates both the cluster connection and a handle to the application table.
    HTableInterface table = null;
    try {
      // retrieve a handle to the target table.
      table = new HTable(getConf(), TABLE_NAME);
      // describe the data we want to write.
      Put put = new Put(Bytes.toBytes("someRow"));
      put.add(CF, Bytes.toBytes("qual"), Bytes.toBytes(42.0d));
      // send the data.
      table.put(put);
    } finally {
      // close everything down
      if (table != null) table.close();
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new OldClientAPIExample(), argv);
    System.exit(ret);
  }
}
