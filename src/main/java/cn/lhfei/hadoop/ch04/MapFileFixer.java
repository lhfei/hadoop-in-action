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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 30, 2014
 */

public class MapFileFixer {

	public static void main(String[] args) throws Exception {
		String mapUri = args[0];

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create(mapUri), conf);
		Path map = new Path(mapUri);
		Path mapData = new Path(map, MapFile.DATA_FILE_NAME);

		// Get key and value types from data sequence file
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);
				
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		reader.close();

		// Create the map file index file
		long entries = MapFile.fix(fs, map, keyClass, valueClass, false, conf);
		System.out.printf("Created MapFile %s with %d entries\n", map, entries);
	}

}
