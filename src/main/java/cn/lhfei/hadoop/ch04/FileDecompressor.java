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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 27, 2014
 */

public class FileDecompressor {

	/**
	 * use case: % hadoop FileDecompressor file.gz
	 * @param args
	 */
	public static void main(String[] args) {
		FileSystem fs = null;
		String uri = args[0];
		Path inputPath = null;
		Configuration conf = new Configuration();
		CompressionCodecFactory factory = null;
		
		InputStream in = null;
		OutputStream out = null;
		
		try {
			fs = FileSystem.get(URI.create(uri), conf);
			inputPath = new Path(uri);
			factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodec(inputPath);
			if (codec == null) {
				System.err.println("No codec found for " + uri);
				System.exit(1);
			}
			
			String outputUri = CompressionCodecFactory.
					removeSuffix(uri, codec.getDefaultExtension());
			
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			
			IOUtils.copyBytes(in, out, conf);
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}

}
