package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class CassandraHBaseClient extends DB {
    
    private static final int INTERVAL = 100;
    
    private DB myCassandra;
    private DB myHBase;
    private DB myCurrent;
    
    private Map<String, Integer> myOperations;
    private int myCount;
    
    private void resetState() {
        myCount = 0;
        myOperations.put("READ", 0);
        myOperations.put("SCAN", 0);
        myOperations.put("UPDATE", 0);
        myOperations.put("INSERT", 0);
        myOperations.put("DELETE", 0);
    }
    
    @Override
    public void init() throws DBException {
        myOperations = new HashMap<String, Integer>();
        resetState();
        
        myCassandra = new CassandraClient8();
        myCassandra.init();
        myHBase = new HBaseClient();
        myHBase.init();
        
        //set default primary
        myCurrent = myHBase;
    }
    
    @Override
    public void cleanup() throws DBException {
        myCassandra.cleanup();
        myHBase.cleanup();
    }
    
    private void recordAndEvaluateSwitch(String type) {
        myOperations.put(type, myOperations.get(type) + 1);
        myCount++;
        
        if(myCount >= INTERVAL - 1) {
            // Look at stats and decide whether to switch here
            
            resetState();
        }
        
    }

    @Override
    public int read (String table,
                     String key,
                     Set<String> fields,
                     HashMap<String, String> result) {
        recordAndEvaluateSwitch("READ");
        return myCurrent.read(table, key, fields, result);
    }


    @Override
    public int scan (String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, String>> result) {
        recordAndEvaluateSwitch("SCAN");
        return myCurrent.scan(table, startkey, recordcount, fields, result);
    }


    @Override
    public int update (String table, String key, HashMap<String, String> values) {
        recordAndEvaluateSwitch("UPDATE");
        return 0;
    }


    @Override
    public int insert (String table, String key, HashMap<String, String> values) {
        recordAndEvaluateSwitch("INSERT");
        return 0;
    }


    @Override
    public int delete (String table, String key) {
        recordAndEvaluateSwitch("DELETE");
        return 0;
    }

}
