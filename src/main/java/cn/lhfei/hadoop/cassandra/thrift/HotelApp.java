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
package cn.lhfei.hadoop.cassandra.thrift;

import static cn.lhfei.hadoop.cassandra.thrift.Constants.CL;
import static cn.lhfei.hadoop.cassandra.thrift.Constants.UTF8;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Jun 18, 2016
 */
public class HotelApp {
	private static final Logger LOG = LoggerFactory.getLogger(HotelApp.class);

	public static void main(String[] args) throws Exception {

		// first put all of the data in the database
		new Prepopulate().prepopulate();
		LOG.debug("** Database filled. **");

		// now run our client
		LOG.debug("** Starting hotel reservation app. **");
		HotelApp app = new HotelApp();

		// find a hotel by city--try Scottsdale or New York...
		List<Hotel> hotels = app.findHotelByCity("Scottsdale", "AZ");
		// List<Hotel> hotels = app.findHotelByCity("New York", "NY");
		LOG.debug("Found hotels in city. Results: " + hotels.size());

		// choose one
		Hotel h = hotels.get(0);

		LOG.debug("You picked " + h.name);

		// find Points of Interest for selected hotel
		LOG.debug("Finding Points of Interest near " + h.name);
		List<POI> points = app.findPOIByHotel(h.name);

		// choose one
		if (!points.isEmpty()) {
			POI poi = points.get(0);
			LOG.debug("Hm... " + poi.name + ". " + poi.desc + "--Sounds fun!");
		}

		LOG.debug("Now to book a room...");

		// show availability for a date
		// left as an exercise...

		// create reservation
		// left as an exercise...

		LOG.debug("All done.");
	}

	// use column slice to get from Super Column
	public List<POI> findPOIByHotel(String hotel) throws Exception {

		// /query
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(hotel.getBytes());
		sliceRange.setFinish(hotel.getBytes());
		predicate.setSlice_range(sliceRange);

		// read all columns in the row
		String scFamily = "PointOfInterest";
		ColumnParent parent = new ColumnParent(scFamily);

		KeyRange keyRange = new KeyRange();
		keyRange.start_key = bytes("");
		keyRange.end_key = bytes("");

		List<POI> pois = new ArrayList<POI>();

		// instead of a simple list, we get a map whose keys are row keys
		// and the values the list of columns returned for each
		// only row key + first column are indexed
		Connector cl = new Connector();
		Cassandra.Client client = cl.connect();
		List<KeySlice> slices = client.get_range_slices(parent, predicate, keyRange, CL);

		for (KeySlice slice : slices) {
			List<ColumnOrSuperColumn> cols = slice.columns;

			POI poi = new POI();
			poi.name = new String(ByteBufferUtil.string(slice.key));

			for (ColumnOrSuperColumn cosc : cols) {
				SuperColumn sc = cosc.super_column;

				List<Column> colsInSc = sc.columns;

				for (Column c : colsInSc) {
					String colName = new String(c.name.array(), UTF8);
					if (colName.equals("desc")) {
						poi.desc = new String(c.value.array(), UTF8);
					}
					if (colName.equals("phone")) {
						poi.phone = new String(c.value.array(), UTF8);
					}
				}

				LOG.debug("Found something neat nearby: " + poi.name + ". \nDesc: " + poi.desc + ". \nPhone: "
						+ poi.phone);
				pois.add(poi);
			}
		}

		cl.close();
		return pois;
	}

	// uses key range
	public List<Hotel> findHotelByCity(String city, String state) throws Exception {

		LOG.debug("Seaching for hotels in " + city + ", " + state);

		String key = city + ":" + state.toUpperCase();

		// /query
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(new byte[0]);
		sliceRange.setFinish(new byte[0]);
		predicate.setSlice_range(sliceRange);

		// read all columns in the row
		String columnFamily = "HotelByCity";
		ColumnParent parent = new ColumnParent(columnFamily);

		KeyRange keyRange = new KeyRange();
		keyRange.setStart_key(key.getBytes());
		keyRange.setEnd_key("".getBytes()); // just outside lexical range
		keyRange.count = 5;

		Connector cl = new Connector();
		Cassandra.Client client = cl.connect();
		List<KeySlice> keySlices = client.get_range_slices(parent, predicate, keyRange, CL);

		List<Hotel> results = new ArrayList<Hotel>();

		for (KeySlice ks : keySlices) {
			List<ColumnOrSuperColumn> coscs = ks.columns;
			LOG.debug(new String("Using key " + ks.key));

			for (ColumnOrSuperColumn cs : coscs) {

				Hotel hotel = new Hotel();
				hotel.name = ByteBufferUtil.string(cs.column.name);
				hotel.city = city;
				hotel.state = state;

				results.add(hotel);
				LOG.debug("Found hotel result for " + hotel.name);
			}
		}
		// /end query
		cl.close();

		return results;
	}
}
