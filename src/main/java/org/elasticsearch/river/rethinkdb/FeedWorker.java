package org.elasticsearch.river.rethinkdb;

import com.rethinkdb.Cursor;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
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
                    cursor = r.table(changeRecord.table).changes().runForCursor(connection);
                    if (backfillRequired) {
                        backfill();
                    }
                    int counter = 0;
                    while (cursor.hasNext()) {
                        Map<String, Object> change = cursor.next();
                        updateES(change);
                        counter++;
                        if(counter % 10 == 0){
                            logger.info("Synced {} documents", counter);
                        }
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

    private boolean updateES(Map<String, Object> change) {
        Map<String, Object> newVal = (Map) change.get("new_val");
        Map<String, Object> oldVal = (Map) change.get("old_val");
        if(newVal != null) {
            client.prepareIndex(
                    changeRecord.targetIndex,
                    changeRecord.targetType,
                    newVal.get(primaryKey).toString())
                    .setSource(newVal)
                    .execute();
            return false;
        }else{
            client.prepareDelete(
                    changeRecord.targetIndex,
                    changeRecord.targetType,
                    oldVal.get(primaryKey).toString())
                    .execute();
            return true;
        }
    }

    private int synchronizeBulk(BulkRequestBuilder bulkRequest, HashSet<String> failureReasons) {
        int failed = 0;
        BulkResponse response = bulkRequest.get();
        if (response.hasFailures()) {
            logger.error("Encountered errors backfilling");
            logger.error(response.buildFailureMessage());
            for(BulkItemResponse ir : response.getItems()){
                if (ir.isFailed()) {
                    failed++;
                    failureReasons.add(ir.getFailureMessage());
                }
            }
        }
        return failed;
    }

    private void backfill() throws IOException {
        RethinkDBConnection backfillConnection = r.connect(river.hostname, river.port, river.authKey);
        backfillConnection.use(changeRecord.db);
        try {
            logger.info("Beginning backfill of documents");
            // totalSize is purely for the purposes of printing progress, and may be inaccurate since documents can be
            // inserted while we're backfilling
            int totalSize = r.table(changeRecord.table).count().run(backfillConnection).intValue();
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int attempted = 0, failed = 0;
            HashSet<String> failureReasons = new HashSet<>();
            int oldDecile = 0, newDecile;
            Cursor cursor = r.table(changeRecord.table).runForCursor(backfillConnection);
            while (cursor.hasNext()){
                Map<String, Object> doc = (Map<String, Object>) cursor.next();
                newDecile = (attempted * 100) / totalSize / 10;
                if (newDecile != oldDecile) {
                    logger.info("backfill {}0% complete ({} documents)", newDecile, attempted);
                    oldDecile = newDecile;
                }
                if (attempted > 0 && attempted % 1000 == 0) {
                    failed += synchronizeBulk(bulkRequest, failureReasons);
                    bulkRequest = client.prepareBulk();
                }
                bulkRequest.add(client.prepareIndex(
                                changeRecord.targetIndex,
                                changeRecord.targetType,
                                doc.get(primaryKey).toString())
                                .setSource(doc)
                );
                attempted += 1;
            }
            if (bulkRequest.numberOfActions() > 0) {
                failed += synchronizeBulk(bulkRequest, failureReasons);
            }
            if (failed > 0) {
                logger.info("Attempted to backfill {} items, {} succeeded and {} failed.",
                        attempted, attempted - failed, failed);
                logger.info("Unique failure reasons were: {}", failureReasons.toString());
                backfillRequired = true;
            } else {
                logger.info("Backfilled {} items. Turning off backfill in settings", attempted);
                backfillRequired = false;
            }
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
        return tableInfo.get("primary_key").toString();
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
