package org.elasticsearch.river.rethinkdb;

import com.rethinkdb.Cursor;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


class FeedWorker implements Runnable {

    private RethinkDBRiver river;
    private final RethinkDB r = RethinkDB.r;
    private ChangeRecord changeRecord;
    private RethinkDBConnection connection;
    private Cursor<Map<String,Object>> cursor;
    private String primaryKey;
    private boolean backfillRequired;
    private Client client;
    private final ESLogger logger;
    private int backoff = 250;

    public FeedWorker(RethinkDBRiver river, ChangeRecord changeRecord, Client client){
        this.river = river;
        this.changeRecord = changeRecord;
        this.backfillRequired = changeRecord.backfill;
        this.client = client;
        logger = ESLoggerFactory.getLogger("[" + changeRecord.db + "." + changeRecord.table + "] ", "river.rethinkdb.feedworker");
    }

    private RethinkDBConnection connect(){
        RethinkDBConnection conn = r.connect(river.hostname, river.port, river.authKey);
        conn.use(changeRecord.db);
        return conn;
    }

    private void close(){
        if (cursor != null && !cursor.isClosed()){
            try {
                cursor.close();
            }catch(Exception e){}
        }
        if (connection != null && !connection.isClosed()){
            try {
                connection.close();
            }catch(Exception e){}
        }
        cursor = null;
        connection = null;
    }

    @Override
    public void run() {
        try {
            connection = connect();
            primaryKey = getPrimaryKey();
            while (!river.closed) {
                try {
                    cursor = changeRecord.query.runForCursor(connection);
                    if (backfillRequired) {
                        backfill();
                    }
                    int counter = 0;
                    while (cursor.hasNext()) {
                        Map<String, Object> change = cursor.next();
                        counter++;
                        if(counter % 10 == 1){
                            logger.info("Synced {} documents", counter);
                        }
                        client.prepareIndex(
                                changeRecord.targetIndex,
                                changeRecord.targetType,
                                (String) change.get(primaryKey))
                                .setSource(change)
                                .execute();
                    }
                } catch (RethinkDBException e) {
                    logger.error("Worker has a problem: " + e.getMessage());
                    if (isRecoverableError(e)) {
                        logger.info("I think this is recoverable. Hang on a second...");
                        reconnect();
                    } else {
                        logger.info("This probably isn't recoverable, bailing.");
                        throw e;
                    }
                }
            }
        } catch (InterruptedException ie) {
            // We are just being shut down by the river, don't do anything
        } catch (Exception e){
            if(!river.closed) {
                logger.error("failed due to exception", e);
            }
        } finally {
            logger.info("thread shutting down");
            close();
        }
    }

    private void backfill() throws IOException {
        RethinkDBConnection backfillConnection = r.connect(river.hostname, river.port, river.authKey);
        backfillConnection.use(changeRecord.db);
        try {
            logger.info("Beginning backfill of documents");
            // totalSize is purely for the purposes of printing progress, and may be inaccurate since documents can be
            // inserted while we're backfilling
            int totalSize = r.table(changeRecord.table).count().run(backfillConnection).intValue();
            int tenthile = (totalSize + 9) / 10; // ceiling integer division by 10
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int i = 1;
            for (Map<String, Object> doc : r.table(changeRecord.table).run(backfillConnection)) {
                if (i % tenthile == 0) {
                    logger.info("backfill {}% complete ({} documents)", (i / tenthile) * 10, i);
                }
                if (i % 100 == 0) {
                    bulkRequest.execute();
                    bulkRequest = client.prepareBulk();
                }
                bulkRequest.add(client.prepareIndex(
                                changeRecord.targetIndex,
                                changeRecord.targetType,
                                (String) doc.get(primaryKey))
                                .setSource(doc)
                );
                i += 1;
            }
            bulkRequest.execute();
            logger.info("Backfilled {} items. Turning off backfill in settings", i);
            backfillRequired = false;
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                      .startObject("rethinkdb")
                        .startObject("databases")
                          .startObject(changeRecord.db)
                            .startObject(changeRecord.table)
                            .field("backfill", backfillRequired)
                            .endObject()
                          .endObject()
                        .endObject()
                      .endObject()
                    .endObject();
            client.prepareUpdate("_river", river.riverName().name(), "_meta")
                    .setRetryOnConflict(river.changeRecords.totalSize + 1) // only other backfilling threads should conflict
                    .setDoc(builder)
                    .execute();
            backfillConnection.close();
        } finally {
            backfillConnection.close();
        }
    }

    @SuppressWarnings("unchecked")
    private String getPrimaryKey() {
        Map<String, Object> tableInfo = (Map) r.db(changeRecord.db).table(changeRecord.table).info().run(connection);
        return (String) tableInfo.get("primary_key");
    }

    private boolean isRecoverableError(RethinkDBException exc){
        String msg = exc.getMessage();
        return !river.closed && (// Don't try to recover if the River is shutting down
                msg.matches(".*?Master for shard \\[.*\\) not available.*") // happens immediately after the db starts up, temporary
                || msg.matches(".*?Error receiving data.*") // happens when the database shuts down while we're waiting
                || msg.matches(".*?Query interrupted.*")    // happens when a query is killed? maybe.
                || msg.matches(".*?Broken pipe.*")
                );
    }

    private void reconnect() throws InterruptedException {
        Thread.sleep(backoff);
        logger.info("Attempting to reconnect to {}:{}", river.hostname, river.port);
        for(backoff = Math.min(backoff*2, 30000);;backoff = Math.min(backoff*2, 30000)){
            try {
                close();
                connection = connect();
            }catch(RethinkDBException e){
                logger.error("Reconnect failed, waiting {}ms before trying again", backoff);
                Thread.sleep(backoff);
                continue;
            }
            logger.info("Reconnection successful.");
            break;
        }
    }
}
