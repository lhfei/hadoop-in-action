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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 30, 2014
 */

public class MapFileWriteDemo {

	private static final String[] DATA = { "One, two, buckle my shoe",
			"Three, four, shut the door", "Five, six, pick up sticks",
			"Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = null;

		IntWritable key = new IntWritable();
		Text value = new Text();
		MapFile.Writer writer = null;
		try {
			fs = FileSystem.get(URI.create(uri), conf);
			/*writer = new MapFile.Writer(conf, fs, uri, key.getClass(),
					value.getClass());*/
			
			writer = new MapFile.Writer(conf, new Path(uri), Writer.keyClass(key.getClass()), 
					Writer.valueClass(value.getClass()));

			for (int i = 0; i < 1024; i++) {
				key.set(i + 1);
				value.set(DATA[i % DATA.length]);
				writer.append(key, value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

}
