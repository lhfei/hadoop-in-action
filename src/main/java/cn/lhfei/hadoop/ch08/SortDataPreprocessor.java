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

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 9, 2015
 */
public class SortDataPreprocessor extends Configured implements Tool {
	
	private static final Logger log = LoggerFactory.getLogger(SortDataPreprocessor.class);

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, super.getConf(), args);
		if (job == null) {
			return -1;
		}
		
	    job.setMapperClass(CleanerMapper.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(0);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setCompressOutput(job, true);
	    SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
	    SequenceFileOutputFormat.setOutputCompressionType(job,
	        CompressionType.BLOCK);

	    return job.waitForCompletion(true) ? 0 : 1;
	}

	static class CleanerMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			parser.parse(value);
			if (parser.isValidTemperature()) {
				context.write(new IntWritable(parser.getAirTemperature()),
						value);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args == null || args.length != 2) {
			args = new String[]{"src/main/resources/input/ncdc/all", "src/main/resources/08/all-seq"};
		}
		
		FileUtils.deleteDirectory(args[1]);
		
		int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
		System.exit(exitCode);
	}
}
