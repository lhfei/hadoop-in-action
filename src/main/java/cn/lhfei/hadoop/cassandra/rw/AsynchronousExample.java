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
package cn.lhfei.hadoop.cassandra.rw;

import cn.lhfei.hadoop.cassandra.driver.DriverConnector;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Jun 20, 2016
 */
public class AsynchronousExample extends DriverConnector {

    private static Logger log = LoggerFactory.getLogger(AsynchronousExample.class);

    public AsynchronousExample() {
    }

    public ResultSetFuture getRows() {
        QueryBuilder.select().all().from("","" );
        Select query = QueryBuilder.select().all().from("simplex", "songs");
        return super.getSession().executeAsync(query);
    }

    public static void main(String[] args) {
        AsynchronousExample client = new AsynchronousExample();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        ResultSetFuture results = client.getRows();
        for (Row row : results.getUninterruptibly()) {
            log.info("Title: {}, Album: {}, Artlist: {}",
                    row.getString("title"),
                    row.getString("album"), row.getString("artist"));
        }
        client.dropSchema("simplex");
        client.close();

    }
}