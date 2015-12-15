/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.lhfei.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lhfei.hadoop.hbase.common.HBaseHelper;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Dec 15, 2015
 */
public class RESTApp {
	private static final Logger log = LoggerFactory.getLogger(RESTApp.class);

	public static void main(String[] args) {
	    try {
			Configuration conf = HBaseConfiguration.create();

			HBaseHelper helper = HBaseHelper.getHelper(conf);
			helper.dropTable("test_table");
			helper.createTable("test_table", "music", "wallpaper", "others");
			System.out.println("Adding rows to table...");
			helper.fillTable("test_table", 1, 5, 10, "imtei");

			// vv RestExample
			Cluster cluster = new Cluster();
			cluster.add("centos10-82.letv.cn", 8080); //RestExample-1-Cluster Set up a cluster list adding all known REST server hosts.

			Client client = new Client(cluster); //RestExample-2-Client Create the client handling the HTTP communication.

			RemoteHTable table = new RemoteHTable(client, "test_table"); //RestExample-3-Table Create a remote table instance, wrapping the REST access into a familiar interface.

			Get get = new Get(Bytes.toBytes("row-30")); //RestExample-4-Get Perform a get operation as if it were a direct HBase connection.
			get.addColumn(Bytes.toBytes("music"), Bytes.toBytes("col-3"));
			Result result1 = table.get(get);

			log.info("Get result1: " ,result1);

			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes("row-10"));
			scan.setStopRow(Bytes.toBytes("row-15"));
			scan.addColumn(Bytes.toBytes("music"), Bytes.toBytes("col-5"));
			ResultScanner scanner = table.getScanner(scan); //RestExample-5-Scan Scan the table, again, the same approach as if using the native Java API.

			for (Result result2 : scanner) {
				log.info("Scan row[{}", Bytes.toString(result2.getRow()), "]: {}", result2);
			}
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}

	}

}
