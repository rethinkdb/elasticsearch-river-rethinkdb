/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.rethinkdb;

//import org.rethinkdb.RethinkDB;
//import org.rethinkdb.RethinkDBConnection;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RethinkDBRiver extends AbstractRiverComponent implements River {

    private final Client client;

    //private final RethinkDBConnection reconn;
    public final String hostname;
    public final int port;
    public final List<String> dbs;

    private volatile boolean closed = false;

    private volatile Thread thread;

    @SuppressWarnings({"unchecked"})
    @Inject
    public RethinkDBRiver(
            RiverName riverName,
            RiverSettings settings,
            Client client) {
        super(riverName, settings);
        this.client = client;

        Map<String, Object> rdbSettings = jsonGet(
                settings.settings(), "rethinkdb", new HashMap<String, Object>());

        hostname = jsonGet(rdbSettings, "host", "localhost");
        port = jsonGet(rdbSettings, "port", 28015);
        dbs = jsonGet(rdbSettings, "databases", new ArrayList<String>());
    }

    @Override
    public void start() {
    }

    @Override
    public void close() {
    }

    private class Consumer implements Runnable {
        @Override
        public void run() {
        }
    }

    @SuppressWarnings({"unchecked"})
    public static <V> V jsonGet(
            Map<String, Object> map,
            String key,
            V defaultValue){
        return map.containsKey(key) ? ((Map<String, V>)map).get(key) : defaultValue;
    }
}
