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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IOUtils;


/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 10, 2015
 */
public class NcdcStationMetadata {
	private Map<String, String> stationIdToName = new HashMap<String, String>();

	public void initialize(File file) throws IOException {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
			NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
			String line;
			while ((line = in.readLine()) != null) {
				if (parser.parse(line)) {
					stationIdToName.put(parser.getStationId(),
							parser.getStationName());
				}
			}
		} finally {
			IOUtils.closeStream(in);
		}
	}

	public String getStationName(String stationId) {
		String stationName = stationIdToName.get(stationId);
		if (stationName == null || stationName.trim().length() == 0) {
			return stationId; // no match: fall back to ID
		}
		return stationName;
	}

	public Map<String, String> getStationIdToNameMap() {
		return Collections.unmodifiableMap(stationIdToName);
	}
}
