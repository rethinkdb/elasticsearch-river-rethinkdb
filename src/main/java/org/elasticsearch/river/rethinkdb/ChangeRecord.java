package org.elasticsearch.river.rethinkdb;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.query.RqlQuery;

import java.util.Map;


public class ChangeRecord {
    public final String table;
    public final String db;
    public final boolean backfill;
    public final String targetIndex;
    public final String targetType;

    public ChangeRecord(String db, String table, Map<String, Object> options){
        this.db = db;
        this.table = table;
        this.backfill = (boolean) options.getOrDefault("backfill", false);
        this.targetIndex = (String) options.getOrDefault("index", db);
        this.targetType = (String) options.getOrDefault("type", table);
    }

    @Override
    public String toString(){
        return "ChangeRecord(" + db + "," + table + "," +
                (backfill ? "backfill,": "no backfill,") +
                (!targetIndex.equals(db) ? "index=" + targetIndex + ",": "") +
                (!targetType.equals(table) ? "type=" + targetType : "");
    }
}
