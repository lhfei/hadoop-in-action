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

package cn.lhfei.hadoop.ch05.v3;

import org.apache.hadoop.io.Text;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 30, 2014
 */

public class NcdcRecordParser {
	private static final int MISSING_TEMPERATURE = 9999;

	private String year;
	private int airTemperature;
	private String quality;

	public void parse(String record) {
		year = record.substring(15, 19);
		String airTemperatureString;
		// Remove leading plus sign as parseInt doesn't like them
		if (record.charAt(87) == '+') {
			airTemperatureString = record.substring(88, 92);
		} else {
			airTemperatureString = record.substring(87, 92);
		}
		airTemperature = Integer.parseInt(airTemperatureString);
		quality = record.substring(92, 93);
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	public boolean isValidTemperature() {
		return airTemperature != MISSING_TEMPERATURE
				&& quality.matches("[01459]");
	}

	public String getYear() {
		return year;
	}

	public int getAirTemperature() {
		return airTemperature;
	}
}
