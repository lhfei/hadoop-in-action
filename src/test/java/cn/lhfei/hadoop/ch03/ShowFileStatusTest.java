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
package cn.lhfei.hadoop.ch03;

import java.io.IOException;
import java.io.OutputStream;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Mar 3, 2015
 */
public class ShowFileStatusTest {

	private MiniDFSCluster cluster; // use an in-process HDFS cluster for
									// testing
	private FileSystem fs;

	@Before
	public void setUp() throws IOException {
		Configuration conf = new Configuration();
		if (System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", "/tmp");
		}

		// cluster = new MiniDFSCluster(conf, 1, true, null);
		cluster = new MiniDFSCluster.Builder(conf).build();
		fs = cluster.getFileSystem();
		
		OutputStream out = fs.create(new Path("/dir/file"));
		out.write("content".getBytes("UTF-8"));
		
		out.close();
	}

	@After
	public void tearDown() throws IOException {
		if(fs != null){
			fs.close();
		}
		if(cluster != null){
			cluster.shutdown();
		}
	}
	
	@Test
	public void fileStatusForFile() throws IOException {
		Path path = new Path("/dir/file");
		FileStatus stat = fs.getFileStatus(path);
		
		Assert.assertEquals(stat.getPath().toUri().getPath(), "/dir/file");
		Assert.assertTrue(stat.isFile());
	}
}
