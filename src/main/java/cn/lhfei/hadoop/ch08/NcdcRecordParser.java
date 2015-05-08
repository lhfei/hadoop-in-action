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
package cn.lhfei.hadoop.ch08;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 9, 2015
 */
public class NcdcRecordParser {
	private static final int MISSING_TEMPERATURE = 9999;

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyyMMddHHmm");

	private String stationId;
	private String observationDateString;
	private String year;
	private String airTemperatureString;
	private int airTemperature;
	private boolean airTemperatureMalformed;
	private String quality;

	public void parse(String record) {
		stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
		observationDateString = record.substring(15, 27);
		year = record.substring(15, 19);
		airTemperatureMalformed = false;
		// Remove leading plus sign as parseInt doesn't like them
		if (record.charAt(87) == '+') {
			airTemperatureString = record.substring(88, 92);
			airTemperature = Integer.parseInt(airTemperatureString);
		} else if (record.charAt(87) == '-') {
			airTemperatureString = record.substring(87, 92);
			airTemperature = Integer.parseInt(airTemperatureString);
		} else {
			airTemperatureMalformed = true;
		}
		airTemperature = Integer.parseInt(airTemperatureString);
		quality = record.substring(92, 93);
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	public boolean isValidTemperature() {
		return !airTemperatureMalformed
				&& airTemperature != MISSING_TEMPERATURE
				&& quality.matches("[01459]");
	}

	public boolean isMalformedTemperature() {
		return airTemperatureMalformed;
	}

	public boolean isMissingTemperature() {
		return airTemperature == MISSING_TEMPERATURE;
	}

	public String getStationId() {
		return stationId;
	}

	public Date getObservationDate() {
		try {
			return DATE_FORMAT.parse(observationDateString);
		} catch (ParseException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public String getYear() {
		return year;
	}

	public int getYearInt() {
		return Integer.parseInt(year);
	}

	public int getAirTemperature() {
		return airTemperature;
	}

	public String getAirTemperatureString() {
		return airTemperatureString;
	}

	public String getQuality() {
		return quality;
	}
}
