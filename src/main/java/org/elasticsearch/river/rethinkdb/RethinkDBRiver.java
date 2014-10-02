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

import com.rethinkdb.RethinkDB;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class RethinkDBRiver extends AbstractRiverComponent implements River {

    private static final RethinkDB r = RethinkDB.r;
    private final Client client;

    public final String hostname;
    public final int port;
    public final String authKey;
    public final ChangeRecords changeRecords;

    private volatile List<Thread> threads;
    public volatile boolean closed;

    @SuppressWarnings({"unchecked"})
    @Inject
    public RethinkDBRiver(
            RiverName riverName,
            RiverSettings settings,
            Client client) {
        super(riverName, settings);
        this.client = client;

        // Get ye settings from the document:
        // Expected settings document (angle brackets indicate it stands in for a real name)
        // "rethinkdb": {
        //   "host": <hostname (default: "localhost")>,
        //   "port": <port (default: 28015)>,
        //   "auth_key": <authKey (default: "")>,
        //   "databases": {
        //     <dbName>: {
        //       <tableName>: {
        //         "backfill": <whether to backfill (default: true)>,
        //         "index": <indexName (default: <dbName>)>,
        //         "type": <typeName (default: <tableName>)>
        //       },
        //       ... more tables in <dbName>
        //     }
        //     ... more databases
        //   }
        // }
        try {
            Map<String, Object> rdbSettings = jsonGet(
                    settings.settings(), "rethinkdb", new HashMap<>());
            hostname = jsonGet(rdbSettings, "host", "localhost");
            port = jsonGet(rdbSettings, "port", 28015);
            authKey = jsonGet(rdbSettings, "auth_key", "");
            Map<String, ? extends Map<String, ? extends Map<String, Object>>> tables = jsonGet(
                    rdbSettings, "databases", new HashMap<>());

            // Create thy changeRecords to hold the configuration
            changeRecords = new ChangeRecords(tables);
            logger.info("ChangeRecords: {}", changeRecords);
            threads = new ArrayList();
            closed = false;
        }catch(Exception e){
            logger.error("Initializing the RethinkDB River failed. " +
                    " Is your configuration in the right format?" + e.getMessage());
            throw e;
        }
    }

    @Override
    public void start() {
        // Start up all the listening threads
        logger.info("Starting up RethinkDB River for {}:{}", hostname, port);
        for(ChangeRecord changeRecord : changeRecords.getAll()) {
            Thread thread = EsExecutors
                    .daemonThreadFactory(settings.globalSettings(), "rethinkdb_river")
                    .newThread(new FeedWorker(this, changeRecord, client));
            thread.start();
            logger.info("Starting feed watcher for {}.{}", changeRecord.db, changeRecord.table);
            threads.add(thread);
        }
    }

    @Override
    public void close() {
        if(closed){
            return;
        }
        logger.info("Closing RethinkDB River");
        closed = true;
        for(Thread thread: threads){
            thread.interrupt();
        }

    }

    @SuppressWarnings({"unchecked"})
    public static <V> V jsonGet(Map<String,Object> map, String key, V defaultValue){
        return map.containsKey(key) ? (V) map.get(key) : defaultValue;
    }

}
