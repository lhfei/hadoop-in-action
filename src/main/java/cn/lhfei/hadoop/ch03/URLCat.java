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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URLCat Displays files from a Hadoop filesystem on standard output using a URLStreamHandler. <p>
 * 
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 27, 2014
 */

public class URLCat {

	private static final Logger LOGGER = LoggerFactory.getLogger(URLCat.class);
	
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) {
		InputStream in = null;
		FileSystem fs = null;

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", HDFSConstant.FS_DEFAULTFS);

			String uri = HDFSConstant.FS_DEFAULTFS + "/lhfei/IP_List.txt";
			uri = HDFSConstant.FS_DEFAULTFS + "/hive/warehouse/lhfei.db/bank/bank.txt";
			uri = HDFSConstant.FS_DEFAULTFS + "/lhfei/h2o/smalldata/bcwd.csv";

			in = new URL(uri).openStream();

			fs = FileSystem.get(URI.create(uri), conf);

			//IOUtils.copyBytes(in, System.out, 1024,false);


			FSDataInputStream inputStream = fs.open(new Path(uri), 1024);
			String lines = org.apache.commons.io.IOUtils.toString(inputStream);

			Gson gson = new Gson();
			String json = gson.toJson(lines);

			LOGGER.info("============================\t{}", json);

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			IOUtils.closeStream(in);
		}
	}
	
}
