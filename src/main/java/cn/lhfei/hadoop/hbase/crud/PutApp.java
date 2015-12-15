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
package cn.lhfei.hadoop.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
public class PutApp {
	
	private static final Logger log = LoggerFactory.getLogger(PutApp.class);

	public static void main(String[] args) {
	    try {
	    	// create the required configuration.
			Configuration conf = HBaseConfiguration.create();

			HBaseHelper helper = HBaseHelper.getHelper(conf);
			helper.dropTable("testtable");
			helper.createTable("testtable", "colfam1");
			
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf("testtable"));

			// create put with specific row.
			Put put = new Put(Bytes.toBytes("row1")); 

			// add a column, whose name is "colfam1:qual1", to the put.
			put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
			  Bytes.toBytes("val1"));
			
			// add another column, whose name is "colfam1:qual2", to the put.
			put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
			  Bytes.toBytes("val2"));

			// store row with column into the HBase table.
			table.put(put);
			
			// close table and connection instances to free resources.
			table.close(); 
			connection.close();
			
			helper.close();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}

}
