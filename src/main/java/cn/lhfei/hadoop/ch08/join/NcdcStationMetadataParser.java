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

import org.apache.hadoop.io.Text;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 10, 2015
 */
public class NcdcStationMetadataParser {
	private String stationId;
	private String stationName;

	public boolean parse(String record) {
		if (record.length() < 42) { // header
			return false;
		}
		String usaf = record.substring(0, 6);
		String wban = record.substring(7, 12);
		stationId = usaf + "-" + wban;
		stationName = record.substring(13, 42);
		try {
			Integer.parseInt(usaf); // USAF identifiers are numeric
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public boolean parse(Text record) {
		return parse(record.toString());
	}

	public String getStationId() {
		return stationId;
	}

	public String getStationName() {
		return stationName;
	}
}
