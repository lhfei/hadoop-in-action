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

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Jan 20, 2016
 */

public class StandardDeviationDriver extends Configured implements Tool {
	private static final Logger log = LoggerFactory.getLogger(StandardDeviationDriver.class);
	
	private static final String MEAN_FILE_NAME = "mean.txt";
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		log.info("Input Path: {}", args[0]);
		log.info("Out Path: {}", args[1]);
		
		Configuration conf = super.getConf();
		
		Job meanJob = Job.getInstance(conf);
		
		meanJob.setMapperClass(StandardMeanMapper.class);
		//meanJob.setReducerClass(StandMeanReducer.class);
		
		meanJob.setOutputKeyClass(LongWritable.class);
		meanJob.setOutputValueClass(LongWritable.class);
		

		FileInputFormat.addInputPath(meanJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(meanJob, new Path(args[2]));
		
		boolean step1 = meanJob.waitForCompletion(true);
		
		if(step1){
			log.info("======= Mean Job done ............................");
			
			Long count = new Long(0l);
			Long sum = new Long(0l);
			
			Counters counter = meanJob.getCounters();
			
			count = counter.findCounter(StandardMeanConstant.MEAN_GROUP_COUNT_GROUP_NAME, StandardMeanConstant.MEAN_GROUP_COUNT_COUNTER_NAME).getValue();
			sum = counter.findCounter(StandardMeanConstant.MEAN_GROUP_SUM_GROUP_NAME, StandardMeanConstant.MEAN_GROUP_SUM_COUNTER_NAME).getValue();
			
			log.info("Mean: size = {}, total = {}", count, sum);
			
			NumberFormat formatter = new DecimalFormat(StandardMeanConstant.STANDARD_MEAN_FORMATER);
			
			double mean = sum / count;
			
			conf.set(StandardMeanConstant.STANDARD_MEAN_NAME, formatter.format(mean));
			
		}
		
		Job deviationJob = Job.getInstance(super.getConf());
		deviationJob.setMapperClass(StandardDeviationMapper.class);
		deviationJob.setReducerClass(StandardDeviationReducer.class);
		
		deviationJob.setOutputKeyClass(LongWritable.class);
		deviationJob.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(deviationJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(deviationJob, new Path(args[1]));
		
		return deviationJob.waitForCompletion(true) ? 0 : 1 ;
	}
	
	
	public static void main(String[] args) throws Exception {
		if(args == null || args.length != 2){
			args = new String[]{"src/test/resources/input/mean/price.txt",
				"src/test/resources/input/mean/price-mean", "src/test/resources/input/mean/price-mean-mean"};
		}
		
		FileUtil.fullyDelete(new File(args[1]));
		FileUtil.fullyDelete(new File(args[2]));
		
		int exitCode = ToolRunner.run(new StandardDeviationDriver(), args);
		
		System.exit(exitCode);
		
	}

}
