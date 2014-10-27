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
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

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

	
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) {
		InputStream in = null;
		
		try {
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096,false);
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			IOUtils.closeStream(in);
		}
	}
	
}
