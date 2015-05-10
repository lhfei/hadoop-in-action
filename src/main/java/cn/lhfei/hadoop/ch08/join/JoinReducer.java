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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 10, 2015
 */
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		Text stationName = new Text(iter.next());
		while (iter.hasNext()) {
			Text record = iter.next();
			Text outValue = new Text(stationName.toString() + "\t"
					+ record.toString());
			context.write(key.getFirst(), outValue);
		}
	}
}
