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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 27, 2014
 */

public class ListStatus {

	static final Logger log = LoggerFactory.getLogger(ListStatus.class);

	public static void main(String[] args) {

		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = null;

		try {
			fs = FileSystem.get(URI.create(uri), conf);

			Path[] paths = new Path[args.length];
			for (int i = 0; i < paths.length; i++) {
				paths[i] = new Path(args[i]);
			}

			FileStatus[] status = fs.listStatus(paths);
			Path[] listPath = FileUtil.stat2Paths(status);

			for (Path p : listPath) {
				log.info(p.toString());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
