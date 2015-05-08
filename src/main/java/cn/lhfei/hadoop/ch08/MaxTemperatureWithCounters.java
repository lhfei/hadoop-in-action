/*
 * Copyright 2010-2014 the original author or authors.
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

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lhfei.hadoop.ch05.v1.MaxTemperatureReducer;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 9, 2015
 */
public class MaxTemperatureWithCounters extends Configured implements Tool {

	private static final Logger log = LoggerFactory
			.getLogger(MaxTemperatureWithCounters.class);

	enum Temperature {
		MISSING, MALFORMED
	}

	class MaxTemperatureMapperWithCounters extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidTemperature()) {
				int airTemperature = parser.getAirTemperature();
				context.write(new Text(parser.getYear()), new IntWritable(
						airTemperature));
			} else if (parser.isMalformedTemperature()) {
				log.info("Ignoring possibly corrupt input: {}", value);

				context.getCounter(Temperature.MALFORMED).increment(1);
			} else if (parser.isMissingTemperature()) {

				context.getCounter(Temperature.MISSING).increment(1);
			}

			// dynamic counter
			context.getCounter("TemperatureQuality", parser.getQuality())
					.increment(1);
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

		job.setMapperClass(MaxTemperatureMapperWithCounters.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if(args == null || args.length != 2){
			args = new String[]{"src/main/resources/input/ncdc/all", "src/main/resources/08"};
		}
		
		int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
		System.exit(exitCode);
	}

}
