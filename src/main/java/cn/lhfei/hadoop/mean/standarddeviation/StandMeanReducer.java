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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Jan 21, 2016
 */

public class StandMeanReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

	private static final Logger log = LoggerFactory.getLogger(StandMeanReducer.class);
	
	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values,
			Reducer<LongWritable, LongWritable, LongWritable, Text>.Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		int total = 0;
		Long sum = 0l;
		
		for(LongWritable val : values){
			sum += val.get();
			total ++;
		}
		
		conf.setInt("MEAN_SIZE", total);
		conf.setLong("MEAN_VAL", sum);
		
		context.write(new LongWritable(total), new Text("" +sum));
		
		
	}

}
