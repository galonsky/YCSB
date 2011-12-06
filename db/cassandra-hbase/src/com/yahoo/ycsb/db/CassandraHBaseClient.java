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
    
    private enum Database {
        HBASE, CASSANDRA
    }
    
    private static final int INTERVAL = 100;
    private static final double HIGH_READ = 67/33;
    private static final double LOW_READ = 17/83;
    
    private Database myPrimaryDatabase;
    private Database mySecondaryDatabase;
    
    private DB myCassandra;
    private DB myHBase;
    private DB myCurrent;
    private DB myOther;

    private int myCount;
    private Queue<Operation> myLastOperations;
    private Queue<Runnable> myPendingWrites;
    
    private void applyWrites(Database d) {
        if(!myPendingWrites.isEmpty())
            System.out.println("Applying pending writes to " + d.toString());
        
        while(!myPendingWrites.isEmpty()) {
            Runnable r = myPendingWrites.poll();
            r.run();
        }
    }
    
    private void switchDatabase(Database d) {
        if(d == myPrimaryDatabase) return;
        
        applyWrites(d);
        
        switch(d) {
            case CASSANDRA:
                myCurrent = myCassandra;
                myOther = myHBase;
                myPrimaryDatabase = Database.CASSANDRA;
                mySecondaryDatabase = Database.HBASE;
                break;
            case HBASE:
                myCurrent = myHBase;
                myOther = myCassandra;
                myPrimaryDatabase = Database.HBASE;
                mySecondaryDatabase = Database.CASSANDRA;
                break;
        }
        
        System.out.println("Switching primary database to " + d.toString());
    }
    
    private Map<Operation, Double> getOperationCount() {
        Map<Operation, Double> map = new HashMap<Operation, Double>();
        for(Operation op : Operation.values()) {
            map.put(op, 0.0);
        }
        for(Operation o : myLastOperations) {
            map.put(o, map.get(o) + 1);
        }
        return map;
    }
    
    @Override
    public void init() throws DBException {
        myLastOperations = new LinkedList<Operation>();
        myPendingWrites = new LinkedList<Runnable>();
        myCount = 0;
        
        Properties prop = getProperties();
        
        myCassandra = new CassandraClient8();
        myCassandra.setProperties(prop);
        myCassandra.init();
        myHBase = new HBaseClient();
        myHBase.setProperties(prop);
        myHBase.init();
        
        //set default primary
        switchDatabase(Database.HBASE);
    }
    
    @Override
    public void cleanup() throws DBException {
        
        applyWrites(mySecondaryDatabase);
        
        myCassandra.cleanup();
        myHBase.cleanup();
    }
    
    private void recordAndEvaluateSwitch(Operation type) {
        myLastOperations.add(type);
        myCount++;
        
        if(myCount > INTERVAL) {
            myLastOperations.poll();
            Map<Operation, Double> map = getOperationCount();
            // Decide whether to switch
            double read = map.get(Operation.READ);
            double update = map.get(Operation.UPDATE);
            
            if(read == 0.0 && update == 0.0)
                return;
            
            double readUpdateRatio = read / update;
            
            if(readUpdateRatio > HIGH_READ)
                switchDatabase(Database.CASSANDRA);
            else if(readUpdateRatio < LOW_READ)
                switchDatabase(Database.HBASE);
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
        
        myPendingWrites.add(new Update(myOther, table, key, values));
        return myCurrent.update(table, key, values);
    }


    @Override
    public int insert (String table, String key, HashMap<String, String> values) {
        recordAndEvaluateSwitch(Operation.INSERT);
        
        myPendingWrites.add(new Insert(myOther, table, key, values));
        return myCurrent.insert(table, key, values);
    }


    @Override
    public int delete (String table, String key) {
        recordAndEvaluateSwitch(Operation.DELETE);
        
        myPendingWrites.add(new Delete(myOther, table, key));
        return myCurrent.delete(table, key);
    }

}
