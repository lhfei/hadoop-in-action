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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import cn.lhfei.hadoop.hbase.common.HBaseHelper;
/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Dec 15, 2015
 */
public class GetApp {

	public static void main(String[] args) {
	    try {
			Configuration conf = HBaseConfiguration.create(); // co GetExample-1-CreateConf Create the configuration.

			HBaseHelper helper = HBaseHelper.getHelper(conf);
			if (!helper.existsTable("testtable")) {
			  helper.createTable("testtable", "colfam1");
			}
			// vv GetExample
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf("testtable")); // co GetExample-2-NewTable Instantiate a new table reference.

			Get get = new Get(Bytes.toBytes("row1")); // co GetExample-3-NewGet Create get with specific row.

			get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co GetExample-4-AddCol Add a column to the get.

			Result result = table.get(get); // co GetExample-5-DoGet Retrieve row with selected columns from HBase.

			byte[] val = result.getValue(Bytes.toBytes("colfam1"),
			  Bytes.toBytes("qual1")); // co GetExample-6-GetValue Get a specific value for the given column.

			System.out.println("Value: " + Bytes.toString(val)); // co GetExample-7-Print Print out the value while converting it back.

			table.close(); // co GetExample-8-Close Close the table and connection instances to free resources.
			connection.close();
			// ^^ GetExample
			helper.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
