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
package cn.lhfei.hadoop.ch08.join;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.lhfei.hadoop.ch05.v1.MaxTemperatureReducer;
import cn.lhfei.hadoop.ch08.JobBuilder;
import cn.lhfei.hadoop.ch08.NcdcRecordParser;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 10, 2015
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFileApi extends
		Configured implements Tool {

	static class StationTemperatureMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			parser.parse(value);
			if (parser.isValidTemperature()) {
				context.write(new Text(parser.getStationId()), new IntWritable(
						parser.getAirTemperature()));
			}
		}
	}

	static class MaxTemperatureReducerWithStationLookup extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private NcdcStationMetadata metadata;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			metadata = new NcdcStationMetadata();
			@SuppressWarnings("deprecation")
			Path[] localPaths = context.getLocalCacheFiles();
			if (localPaths.length == 0) {
				throw new FileNotFoundException(
						"Distributed cache file not found.");
			}
			File localFile = new File(localPaths[0].toString());
			metadata.initialize(localFile);
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			String stationName = metadata.getStationName(key.toString());

			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(new Text(stationName), new IntWritable(maxValue));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(StationTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(
				new MaxTemperatureByStationNameUsingDistributedCacheFileApi(),
				args);
		System.exit(exitCode);
	}

}
