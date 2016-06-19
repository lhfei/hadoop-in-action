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
package cn.lhfei.hadoop.cassandra.thrift;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import static cn.lhfei.hadoop.cassandra.thrift.Constants.KEYSPACE;
import static cn.lhfei.hadoop.cassandra.thrift.Constants.HOST;
import static cn.lhfei.hadoop.cassandra.thrift.Constants.PORT;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Jun 18, 2016
 */
public class Connector {
	TTransport tr = new TSocket(Constants.HOST, Constants.PORT);

	// returns a new connection to our keyspace
	public Cassandra.Client connect() throws TTransportException, TException, InvalidRequestException {
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		Cassandra.Client client = new Cassandra.Client(proto);
		tf.open();
		//client.set_keyspace(KEYSPACE);

		client.send_set_keyspace(KEYSPACE);

		return client;
	}

	public void close() {
		tr.close();
	}

}
