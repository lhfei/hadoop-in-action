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
package cn.lhfei.hadoop.cassandra.rw;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Jun 19, 2016
 */
public class SimpleWriteRead {

	private static final Logger log = LoggerFactory.getLogger(SimpleWriteRead.class);
	
	//set up some constants 
	private static final String UTF8 = "UTF8";
	private static final String HOST = "192.168.118.131";
	private static final int PORT = 9160;
	private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

	//not paying attention to exceptions here
	public static void main(String[] args) throws UnsupportedEncodingException,
			InvalidRequestException, UnavailableException, TimedOutException,
			TException, NotFoundException {

		TTransport tr = new TSocket(HOST, PORT);
		//new default in 0.7 is framed transport
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		Cassandra.Client client = new Cassandra.Client(proto);
		tf.open();
		client.set_keyspace("Keyspace1");

		String cfName = "Standard1";
		ByteBuffer userIDKey = ByteBuffer.wrap("1".getBytes()); //this is a row key

//		Clock clock = new Clock(System.currentTimeMillis());
		
		ColumnParent cp = new ColumnParent(cfName);

		//insert the name column
		log.debug("Inserting row for key {}" , userIDKey.toString());
		Column nameCol = new Column(ByteBuffer.wrap("name".getBytes(UTF8)));
		nameCol.setValue(ByteBuffer.wrap("George Clinton".getBytes()));
		client.insert(userIDKey, cp, nameCol, CL);

		//insert the Age column
		Column ageCol = new Column(ByteBuffer.wrap("name".getBytes(UTF8)));
		ageCol.setValue(ByteBuffer.wrap("69".getBytes()));
		client.insert(userIDKey, cp, ageCol, CL);
				
		log.debug("Row insert done.");

		// read just the Name column
		log.debug("Reading Name Column:");
		
		//create a representation of the Name column
		ColumnPath colPathName = new ColumnPath(cfName);
		colPathName.setColumn("name".getBytes(UTF8));
		Column col = client.get(userIDKey, colPathName,
				CL).getColumn();

		/*LOG.debug("Column name: " + new String(col.name, UTF8));
		LOG.debug("Column value: " + new String(col.value, UTF8));
		LOG.debug("Column timestamp: " + col.clock.timestamp);*/

		//create a slice predicate representing the columns to read
		//start and finish are the range of columns--here, all
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(new byte[0]);
		sliceRange.setFinish(new byte[0]);
		predicate.setSlice_range(sliceRange);

		log.debug("Complete Row:");
		// read all columns in the row
		ColumnParent parent = new ColumnParent(cfName);
		List<ColumnOrSuperColumn> results = 
			client.get_slice(userIDKey, 
					parent, predicate, CL);
		
		//loop over columns, outputting values
		for (ColumnOrSuperColumn result : results) {
			Column column = result.column;
			log.info("Column: {}, Value: {}", new String(column.getName(), UTF8), new String(column.getValue(), UTF8));
		}
		tf.close();
		
		log.debug("All done.");
	}
}