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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import cn.lhfei.hadoop.hbase.common.HBaseHelper;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Dec 15, 2015
 */
public class ThriftApp {

	private static final byte[] TABLE = Bytes.toBytes("testtable");
	private static final byte[] ROW = Bytes.toBytes("testRow");
	private static final byte[] FAMILY1 = Bytes.toBytes("testFamily1");
	private static final byte[] FAMILY2 = Bytes.toBytes("testFamily2");
	private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
	private static final byte[] COLUMN = Bytes.toBytes("testFamily1:testColumn");
	private static final byte[] COLUMN2 = Bytes.toBytes("testFamily2:testColumn2");
	private static final byte[] VALUE = Bytes.toBytes("testValue");

	public static void main(String[] args) {
	    try {
			Configuration conf = HBaseConfiguration.create();
			HBaseHelper helper = HBaseHelper.getHelper(conf);
			helper.dropTable("testtable");

			TTransport transport = new TSocket("10.148.10.82", 9090, 20000);
			TProtocol protocol = new TBinaryProtocol(transport, true, true); // co ThriftExample-1-Conn Create a connection using the Thrift boilerplate classes.
			Hbase.Client client = new Hbase.Client(protocol);
			transport.open();

			ArrayList<ColumnDescriptor> columns = new
			  ArrayList<ColumnDescriptor>();
			ColumnDescriptor cd = new ColumnDescriptor(); // co ThriftExample-2-Desc Create two column descriptor instances.
			cd.name = ByteBuffer.wrap(FAMILY1);
			columns.add(cd);
			cd = new ColumnDescriptor();
			cd.name = ByteBuffer.wrap(FAMILY2);
			columns.add(cd);

			client.createTable(ByteBuffer.wrap(TABLE), columns); // co ThriftExample-3-Table Create the test table.

			ArrayList<Mutation> mutations = new ArrayList<Mutation>();
			mutations.add(new Mutation(false, ByteBuffer.wrap(COLUMN),
			  ByteBuffer.wrap(VALUE), true));
			mutations.add(new Mutation(false, ByteBuffer.wrap(COLUMN2),
			  ByteBuffer.wrap(VALUE), true));
			client.mutateRow(ByteBuffer.wrap(TABLE), ByteBuffer.wrap(ROW), // co ThriftExample-4-Row Insert a test row.
			  mutations, null);

			TScan scan = new TScan();
			int scannerId = client.scannerOpenWithScan(ByteBuffer.wrap(TABLE), // co ThriftExample-5-Scan1 Scan with an instance of TScan. This is the most convenient approach. Print the results in a loop.
			  scan, null);
			for (TRowResult result : client.scannerGet(scannerId)) {
			  System.out.println("No. columns: " + result.getColumnsSize());
			  for (Map.Entry<ByteBuffer, TCell> column :
			    result.getColumns().entrySet()) {
			    System.out.println("Column name: " + Bytes.toString(
			      column.getKey().array()));
			    System.out.println("Column value: " + Bytes.toString(
			      column.getValue().getValue()));
			  }
			}
			client.scannerClose(scannerId);

			ArrayList<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
			columnNames.add(ByteBuffer.wrap(FAMILY1));
			scannerId = client.scannerOpen(ByteBuffer.wrap(TABLE), // co ThriftExample-6-Scan2 Scan again, but with another Thrift method. In addition, set the columns to a specific family only. Also print out the results in a loop.
			  ByteBuffer.wrap(Bytes.toBytes("")), columnNames, null);
			for (TRowResult result : client.scannerGet(scannerId)) {
			  System.out.println("No. columns: " + result.getColumnsSize());
			  for (Map.Entry<ByteBuffer, TCell> column :
			    result.getColumns().entrySet()) {
			    System.out.println("Column name: " + Bytes.toString(
			      column.getKey().array()));
			    System.out.println("Column value: " + Bytes.toString(
			      column.getValue().getValue()));
			  }
			}
			client.scannerClose(scannerId);

			System.out.println("Done.");
			transport.close(); // co ThriftExample-7-Close Close the connection after everything is done.
		} catch (TTransportException e) {
			
			e.printStackTrace();
		} catch (IOError e) {
			
			e.printStackTrace();
		} catch (IllegalArgument e) {
			
			e.printStackTrace();
		} catch (AlreadyExists e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (TException e) {
			
			e.printStackTrace();
		}
	}

}
