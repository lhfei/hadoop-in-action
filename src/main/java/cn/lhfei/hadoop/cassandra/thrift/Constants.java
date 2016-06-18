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

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Jun 18, 2016
 */
public class Constants {
	public static final String CAMBRIA_NAME = "Cambria Suites Hayden";
	public static final String CLARION_NAME = "Clarion Scottsdale Peak";
	public static final String W_NAME = "The W SF";
	public static final String WALDORF_NAME = "The Waldorf=Astoria";

	public static final String UTF8 = "UTF8";
	public static final String KEYSPACE = "Hotelier";
	public static final ConsistencyLevel CL = ConsistencyLevel.ONE;
	public static final String HOST = "127.0.0.1";
	public static final int PORT = 9160;
}
