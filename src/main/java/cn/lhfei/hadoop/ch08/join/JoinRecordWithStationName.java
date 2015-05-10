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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.lhfei.hadoop.ch08.JobBuilder;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 10, 2015
 */
public class JoinRecordWithStationName extends Configured implements Tool {

	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value,
				int numPartitions) {
			return (key.getFirst().hashCode()& Integer.MAX_VALUE)
					% numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			JobBuilder
					.printUsage(this, "<ncdc input> <station input> <output>");
			return -1;
		}

		Job job = Job.getInstance(getConf(), "Join weather records with station names");
		job.setJarByClass(getClass());

		Path ncdcInputPath = new Path(args[0]);
		Path stationInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class,
				JoinRecordMapper.class);
		MultipleInputs.addInputPath(job, stationInputPath,
				TextInputFormat.class, JoinStationMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);

		job.setMapOutputKeyClass(TextPair.class);

		job.setReducerClass(JoinReducer.class);

		job.setOutputKeyClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
		System.exit(exitCode);
	}

}
