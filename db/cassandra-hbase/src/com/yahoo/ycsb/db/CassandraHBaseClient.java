package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class CassandraHBaseClient extends DB {
    
    private enum Operation {
        READ, SCAN, UPDATE, INSERT, DELETE
    }
    
    private static final int INTERVAL = 100;
    
    private DB myCassandra;
    private DB myHBase;
    private DB myCurrent;

    private int myCount;
    private Queue<Operation> myLastOperations;
    
    private Map<Operation, Integer> getOperationCount() {
        Map<Operation, Integer> map = new HashMap<Operation, Integer>();
        for(Operation o : myLastOperations) {
            if(map.containsKey(o))
                map.put(o, map.get(o) + 1);
            else
                map.put(o, 1);
        }
        return map;
    }
    
    @Override
    public void init() throws DBException {
        myLastOperations = new LinkedList<Operation>();
        myCount = 0;
        
        Properties prop = getProperties();
        
        myCassandra = new CassandraClient8();
        myCassandra.setProperties(prop);
        myCassandra.init();
        myHBase = new HBaseClient();
        myHBase.setProperties(prop);
        myHBase.init();
        
        //set default primary
        myCurrent = myHBase;
    }
    
    @Override
    public void cleanup() throws DBException {
        myCassandra.cleanup();
        myHBase.cleanup();
    }
    
    private void recordAndEvaluateSwitch(Operation type) {
        myLastOperations.add(type);
        myCount++;
        
        if(myCount > INTERVAL) {
            myLastOperations.poll();
            Map<Operation, Integer> map = getOperationCount();
            // Decide whether to switch
        }
        
    }

    @Override
    public int read (String table,
                     String key,
                     Set<String> fields,
                     HashMap<String, String> result) {
        recordAndEvaluateSwitch(Operation.READ);
        return myCurrent.read(table, key, fields, result);
    }


    @Override
    public int scan (String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, String>> result) {
        recordAndEvaluateSwitch(Operation.SCAN);
        return myCurrent.scan(table, startkey, recordcount, fields, result);
    }


    @Override
    public int update (String table, String key, HashMap<String, String> values) {
        recordAndEvaluateSwitch(Operation.UPDATE);
        try {
            Update u = new Update(myCassandra, table, key, values);
            Thread t = new Thread(u);
            t.start();
            int hresult = myHBase.update(table, key, values);
            t.join();
            return hresult & u.getResult();
        }
        catch (InterruptedException e) {
            return 1;
        }
    }


    @Override
    public int insert (String table, String key, HashMap<String, String> values) {
        recordAndEvaluateSwitch(Operation.INSERT);
        try {
            Insert u = new Insert(myCassandra, table, key, values);
            Thread t = new Thread(u);
            t.start();
            int hresult = myHBase.insert(table, key, values);
            t.join();
            return hresult & u.getResult();
        }
        catch (InterruptedException e) {
            return 1;
        }
    }


    @Override
    public int delete (String table, String key) {
        recordAndEvaluateSwitch(Operation.DELETE);
        try {
            Delete u = new Delete(myCassandra, table, key);
            Thread t = new Thread(u);
            t.start();
            int hresult = myHBase.delete(table, key);
            t.join();
            return hresult & u.getResult();
        }
        catch (InterruptedException e) {
            return 1;
        }
    }

}
