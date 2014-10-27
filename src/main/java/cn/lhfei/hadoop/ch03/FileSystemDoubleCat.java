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

package cn.lhfei.hadoop.ch03;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 27, 2014
 */

public class FileSystemDoubleCat {

	public static void main(String[] args) {

		String uri = args[0];
		FSDataInputStream in = null;
		FileSystem fs = null;
		Configuration conf = new Configuration();

		try {
			fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));

			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0l); // go back to the start of the file

			IOUtils.copyBytes(in, System.out, 4096, false);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
