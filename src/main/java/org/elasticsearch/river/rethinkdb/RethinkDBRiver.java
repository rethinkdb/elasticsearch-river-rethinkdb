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

import com.rethinkdb.Cursor;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.*;

/**
 *
 */
public class RethinkDBRiver extends AbstractRiverComponent implements River {

    private static final RethinkDB r = RethinkDB.r;
    private final Client client;

    public final String hostname;
    public final int port;
    public final String authKey;
    private final ChangeInfos changeinfos;

    private volatile List<Thread> threads;
    private volatile boolean closed;

    @SuppressWarnings({"unchecked"})
    @Inject
    public RethinkDBRiver(
            RiverName riverName,
            RiverSettings settings,
            Client client) {
        super(riverName, settings);
        this.client = client;

        // Get ye settings from the document:
        Map<String, Object> rdbSettings = jsonGet(
                settings.settings(), "rethinkdb", new HashMap<String, Object>());

        hostname = jsonGet(rdbSettings, "host", "localhost");
        port = jsonGet(rdbSettings, "port", 28015);
        authKey = jsonGet(rdbSettings, "auth_key", "");
        Map<String,? extends List<String>> tables = jsonGet(rdbSettings, "databases", new HashMap());

        // Create thy changeinfos to hold the configuration
        changeinfos = new ChangeInfos(tables);
        threads = new ArrayList();
        closed = false;
    }

    @Override
    public void start() {
        // Start up all the listening threads
        logger.info("Starting up RethinkDB River for {}:{}", hostname, port);
        for(ChangeInfo changeinfo : changeinfos.getAll()) {
            Thread thread = EsExecutors
                    .daemonThreadFactory(settings.globalSettings(), "rethinkdb_river")
                    .newThread(new FeedWorker(changeinfo));
            thread.start();
            logger.info("Starting feed watcher for {}.{}", changeinfo.db, changeinfo.table);
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

    private class FeedWorker implements Runnable {

        private ChangeInfo changeinfo;
        private RethinkDBConnection connection;
        private Cursor<Map<String,Object>> cursor;

        public FeedWorker(ChangeInfo changeinfo){
            this.changeinfo = changeinfo;
        }
        @Override
        public void run() {
            connection = r.connect(hostname, port, authKey);
            connection.use(changeinfo.db);
            cursor = changeinfo.query.runForCursor(connection);
            while(cursor.hasNext()){
                Map<String,Object> change = cursor.next();
                System.out.println(change); // TODO: actually insert this into ES
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    public static <V> V jsonGet(Map<String,Object> map, String key, V defaultValue){
        return map.containsKey(key) ? (V) map.get(key) : defaultValue;
    }

    public class ChangeInfo {
        public final RqlQuery query;
        public final String table;
        public final String db;

        private RethinkDBConnection connection;
        private Cursor<Map<String,Object>> cursor;

        public ChangeInfo(String db,String table){
            this.db = db;
            this.table = table;
            query = r.table(table).changes();
        }
    }

    public class ChangeInfos extends HashMap<String, HashMap<String, ChangeInfo>> {

        public int totalSize;

        public ChangeInfos(Map<String,? extends List<String>> dbs){
            super();
            totalSize = 0;
            for(String db: dbs.keySet()){
                for(String table: dbs.get(db)){
                    this.getOrDefault(db, new HashMap<String, ChangeInfo>())
                            .put(table, new ChangeInfo(db, table));
                    totalSize++;
                }
            }
        }

        public ChangeInfo get(String db, String table){
            return get(db).get(table);
        }

        public List<ChangeInfo> getAll(){
            ArrayList<ChangeInfo> ret = new ArrayList<ChangeInfo>(totalSize);
            for(HashMap tables: values()){
                ret.addAll(tables.values());
            }
            return ret;
        }
    }
}
