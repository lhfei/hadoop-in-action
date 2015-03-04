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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 27, 2014
 */

public class StreamCompressor {

	/**
	 * Use case: % echo "Text" | hadoop StreamCompressor org.apache.hadoop.io.compress.GzipCodec \ | gunzip - Text 
	 * @param args
	 */
	public static void main(String[] args) {
		String codecClassname = args[0];
		
		try {
			Class<?> codecClass = Class.forName(codecClassname);
			
			Configuration conf = new Configuration();
			
			CompressionCodec codec = (CompressionCodec) ReflectionUtils
					.newInstance(codecClass, conf);
			
			CompressionOutputStream out = codec.createOutputStream(System.out);
			
			IOUtils.copyBytes(System.in, out, 4096, false);
			
			out.finish();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
