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
package cn.lhfei.hadoop.mean.standarddeviation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Jan 21, 2016
 */

public class StandardMeanMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

	private static final Logger log = LoggerFactory.getLogger(StandardMeanMapper.class);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {

		log.info("Key: [{}], Value: [{}]", key, value);
		
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.getLocal(conf);
		
		fs.getChildFileSystems();
		
		LongWritable val = new LongWritable(Long.parseLong(value.toString()));
		
		context.write(new LongWritable(0l), val);
		
		context.getCounter(StandardMeanConstant.MEAN_GROUP_COUNT_GROUP_NAME, StandardMeanConstant.MEAN_GROUP_COUNT_COUNTER_NAME).increment(1l);
		context.getCounter(StandardMeanConstant.MEAN_GROUP_SUM_GROUP_NAME, StandardMeanConstant.MEAN_GROUP_SUM_COUNTER_NAME).increment(val.get());
	}

	
}
