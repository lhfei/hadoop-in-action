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
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class StandardDeviationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	private static final Logger log = LoggerFactory.getLogger(StandardDeviationMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {

		String result = "";
		Configuration conf = context.getConfiguration();
		
		NumberFormat formatter = new DecimalFormat(StandardMeanConstant.STANDARD_MEAN_FORMATER);
		
		Double mean = Double.parseDouble(conf.get(StandardMeanConstant.STANDARD_MEAN_NAME));
		Double radix = Double.parseDouble(value.toString());
		
		result += "\t" +formatter.format(radix);
		result += "\t" +formatter.format(radix - mean);
		
		log.info("Key = {}, value = {}", key, value);
		
		
		context.write(key, new Text(result));
	}
	
	

}
