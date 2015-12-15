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
import org.apache.hadoop.hbase.client.Delete;
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
public class DeleteApp {
	
	private static final Logger log = LoggerFactory.getLogger(DeleteApp.class);

	public static void main(String[] args) {
	    try {
			Configuration conf = HBaseConfiguration.create();

			HBaseHelper helper = HBaseHelper.getHelper(conf);
			helper.dropTable("testtable");
			helper.createTable("testtable", 100, "colfam1", "colfam2");
			helper.put("testtable",
			  new String[] { "row1" },
			  new String[] { "colfam1", "colfam2" },
			  new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" },
			  new long[]   { 1, 2, 3, 4, 5, 6 },
			  new String[] { "val1", "val1", "val2", "val2", "val3", "val3" });
			log.info("Before delete call...");
			helper.dump("testtable", new String[]{ "row1" }, null, null);

			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf("testtable"));

			// DeleteExample
			Delete delete = new Delete(Bytes.toBytes("row1")); // DeleteExample-1-NewDel Create delete with specific row.

			delete.setTimestamp(1); // DeleteExample-2-SetTS Set timestamp for row deletes.

			delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // DeleteExample-3-DelColNoTS Delete the latest version only in one column.
			delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), 3); // DeleteExample-4-DelColTS Delete specific version in one column.

			delete.addColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // DeleteExample-5-DelColsNoTS Delete all versions in one column.
			delete.addColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), 2); // DeleteExample-6-DelColsTS Delete the given and all older versions in one column.

			delete.addFamily(Bytes.toBytes("colfam1")); // DeleteExample-7-AddCol Delete entire family, all columns and versions.
			delete.addFamily(Bytes.toBytes("colfam1"), 3); // DeleteExample-8-AddCol Delete the given and all older versions in the entire column family, i.e., from all columns therein.

			table.delete(delete); // DeleteExample-9-DoDel Delete the data from the HBase table.

			// ^^ DeleteExample
			table.close();
			connection.close();
			log.info("After delete call...");
			
			helper.dump("testtable", new String[] { "row1" }, null, null);
			helper.close();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}

	}

}
