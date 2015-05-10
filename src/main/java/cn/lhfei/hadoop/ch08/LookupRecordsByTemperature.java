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
package cn.lhfei.hadoop.ch08;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 10, 2015
 */
public class LookupRecordsByTemperature extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			JobBuilder.printUsage(this, "<path> <key>");
			return -1;
		}
		Path path = new Path(args[0]);
		IntWritable key = new IntWritable(Integer.parseInt(args[1]));

		Reader[] readers = MapFileOutputFormat.getReaders(path, getConf());
		Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
		Text val = new Text();

		Reader reader = readers[partitioner.getPartition(key, val,
				readers.length)];
		Writable entry = reader.get(key, val);
		if (entry == null) {
			System.err.println("Key not found: " + key);
			return -1;
		}
		NcdcRecordParser parser = new NcdcRecordParser();
		IntWritable nextKey = new IntWritable();
		do {
			parser.parse(val.toString());
			System.out.printf("%s\t%s\n", parser.getStationId(),
					parser.getYear());
		} while (reader.next(nextKey, val) && key.equals(nextKey));
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LookupRecordsByTemperature(), args);
		System.exit(exitCode);
	}

}
