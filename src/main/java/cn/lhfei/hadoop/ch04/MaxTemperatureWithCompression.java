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

package cn.lhfei.hadoop.ch04;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.lhfei.hadoop.ch02.MaxTemperatureMapper;
import cn.lhfei.hadoop.ch02.MaxTemperatureReducer;

/**
 * use case: % hadoop MaxTemperatureWithCompression input/ncdc/sample.txt.gz output
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 28, 2014
 */

public class MaxTemperatureWithCompression {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err
					.println("Usage: MaxTemperatureWithCompression <input path> "
							+ "<output path>");
			System.exit(-1);
		}
		
		try {
			Job job = new Job();
			job.setJarByClass(MaxTemperatureWithCompression.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileOutputFormat.setCompressOutput(job, true);
		    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
			
		    job.setMapperClass(MaxTemperatureMapper.class);
		    job.setCombinerClass(MaxTemperatureReducer.class);
		    job.setReducerClass(MaxTemperatureReducer.class);
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
