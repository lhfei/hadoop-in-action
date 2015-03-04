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
package cn.lhfei.hadoop.ch05.v1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import cn.lhfei.hadoop.ch02.MaxTemperature;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Mar 4, 2015
 */
public class MaxTemperatureMapperTest {

	@Test
	public void processesValidRecord() throws IOException {
		Text value = new Text(
				"0043011990999991950051518004+68750+023550FM-12+0382" +
				// Year ^^^^
						"99999V0203201N00261220001CN9999999N9-00111+99999999999");

		MapDriver<LongWritable, Text, Text, IntWritable> driver = new MapDriver<LongWritable, Text, Text, IntWritable>();

		driver.withMapper(new MaxTemperatureMapper())
			.withInputValue(value)
			.withOutput(new Text("1950"), new IntWritable(-11)).runTest();
	}
	
	@Test
	public void ignoresMissingTemperatureRecord() throws IOException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
				// Year ^^^^
				"99999V0203201N00261220001CN9999999N9+99991+99999999999");
				// Temperature ^^^^^
		
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInputValue(value)
		.runTest();
	}
}
