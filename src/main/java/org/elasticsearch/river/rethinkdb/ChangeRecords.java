package org.elasticsearch.river.rethinkdb;

import java.util.*;

public class ChangeRecords implements Iterable {

    public final int totalSize;
    private HashMap<String, HashMap<String, ChangeRecord>> hash;

    public ChangeRecords(Map<String, ? extends Map<String, ? extends Map<String, Object>>> dbs){
        int size = 0;
        hash = new HashMap<>();
        for(String dbName: dbs.keySet()){
            for(String tableName: dbs.get(dbName).keySet()){
                if (!hash.containsKey(dbName)){
                    hash.put(dbName, new HashMap<>());
                }
                hash.get(dbName).put(tableName, new ChangeRecord(dbName, tableName, dbs.get(dbName).get(tableName)));
                size++;
            }
        }
        totalSize = size;
    }

    public ChangeRecord get(String db, String table){
        return hash.get(db).get(table);
    }

    @Override
    public String toString(){
        return "ChangeRecords(" + hash.toString() + ")";
    }

    @Override
    public Iterator<ChangeRecord> iterator(){
        return getAll().iterator();
    }

    public List<ChangeRecord> getAll(){
        ArrayList<ChangeRecord> ret = new ArrayList<>(totalSize);
        for(HashMap tables: hash.values()){
            ret.addAll(tables.values());
        }
        return ret;
    }
}
