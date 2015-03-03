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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 27, 2014
 */

public class FileCopyWithProgress {

	static final Logger log = LoggerFactory
			.getLogger(FileCopyWithProgress.class);

	public static void main(String[] args) {

		String localSrc = args[0];
		String dst = args[1];
		FileSystem fs = null;
		InputStream in = null;
		OutputStream out = null;

		try {
			Configuration conf = new Configuration();
			fs = FileSystem.get(URI.create(localSrc), conf);
			in = new BufferedInputStream(new FileInputStream(localSrc));
			out = fs.create(new Path(dst), new Progressable() {
				@Override
				public void progress() {
					log.info("... ...");
				}
			});

			IOUtils.copyBytes(in, out, 4096, true);

		} catch (FileNotFoundException e) {
			// e.printStackTrace();
			log.error(e.getMessage(), e);
		} catch (IOException e) {
			// e.printStackTrace();
			log.error(e.getMessage(), e);
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
}
