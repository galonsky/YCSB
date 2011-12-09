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
    
    private static final int DEFAULT_INTERVAL = 15;
    private static final double HIGH_READ_DEFAULT = 2.0;
    private static final double LOW_READ_DEFAULT = 0.5;
    private static final String DEBUG_KEY = "CH.debug";
    private static final String INTERVAL_KEY = "CH.interval";
    private static final String LOW_READ_KEY = "CH.lowread";
    private static final String HIGH_READ_KEY = "CH.highread";
    
    private double myHighRead;
    private double myLowRead;
    
    private Database myPrimaryDatabase;
    private Database mySecondaryDatabase;
    
    private DB myCassandra;
    private DB myHBase;
    private DB myCurrent;
    private DB myOther;
    
    private boolean debug;

    private int myCount;
    private int myInterval;
    private Queue<Operation> myLastOperations;
    
    private SecondaryUpdater myUpdater;
    private Thread mySecondaryThread;
    private boolean myUpdaterRunning;
    
    private void switchDatabase(Database d) {
        if(d == myPrimaryDatabase) return;
        
        if(myUpdaterRunning) {
            while(myUpdater.isBusy()); // wait for secondary writes to finish
            stopUpdater();
        }
        
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
    
    private void debugLog(String message) {
        if(debug)
            System.out.println(message);
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
    
    private void startUpdater() {
        debugLog("Starting updater...");
        myUpdater = new SecondaryUpdater(debug);
        mySecondaryThread = new Thread(myUpdater);
        mySecondaryThread.start();
        myUpdaterRunning = true;
        debugLog("Updater Started");
    }
    
    private void stopUpdater() {
        try {
            debugLog("Stopping updater...");
            myUpdater.destroy();
            mySecondaryThread.join();
            myUpdaterRunning = false;
            debugLog("Updater Stopped");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        
    }
    
    @Override
    public void init() throws DBException {
        myLastOperations = new LinkedList<Operation>();
        myCount = 0;
        
        Properties prop = getProperties();
        
        debug = (prop.containsKey(DEBUG_KEY) && prop.getProperty(DEBUG_KEY).equals("true"));
        
        myInterval = DEFAULT_INTERVAL;
        myLowRead = LOW_READ_DEFAULT;
        myHighRead = HIGH_READ_DEFAULT;
        
        if(prop.containsKey(INTERVAL_KEY)) {
            try {
                myInterval = Integer.parseInt(prop.getProperty(INTERVAL_KEY));
            }
            catch(NumberFormatException e) {
                myInterval = DEFAULT_INTERVAL;
            }
        }
        
        if(prop.containsKey(LOW_READ_KEY)) {
            try {
                myLowRead = Double.parseDouble(prop.getProperty(LOW_READ_KEY));
            }
            catch(NumberFormatException e) {
                myLowRead = LOW_READ_DEFAULT;
            }
        }
        
        if(prop.containsKey(HIGH_READ_KEY)) {
            try {
                myHighRead = Double.parseDouble(prop.getProperty(HIGH_READ_KEY));
            }
            catch(NumberFormatException e) {
                myHighRead = HIGH_READ_DEFAULT;
            }
        }
        
        myCassandra = new CassandraClient8();
        myCassandra.setProperties(prop);
        myCassandra.init();
        myHBase = new HBaseClient();
        myHBase.setProperties(prop);
        myHBase.init();
        
        myUpdaterRunning = false;
        
        //set default primary
        switchDatabase(Database.HBASE);
    }
    
    @Override
    public void cleanup() throws DBException {
        if(myUpdaterRunning) {
            while(myUpdater.isBusy()); // wait for secondary writes to finish
            stopUpdater();
        }
        
        myCassandra.cleanup();
        myHBase.cleanup();
    }
    
    private void recordAndEvaluateSwitch(Operation type) {
        
        if(type == Operation.READ || type == Operation.SCAN) { //read only operation
            
            // stop updater if it isn't busy
            if(myUpdaterRunning && !myUpdater.isBusy())
                stopUpdater();
        }
        else { //write
            if(!myUpdaterRunning) //start updater if not running
                startUpdater();
        }
        
        myLastOperations.add(type);
        myCount++;
        
        if(myCount > myInterval) {
            myLastOperations.poll();
            Map<Operation, Double> map = getOperationCount();
            // Decide whether to switch
            double read = map.get(Operation.READ);
            double update = map.get(Operation.UPDATE);
            
            if(read == 0.0 && update == 0.0)
                return;
            
            double readUpdateRatio = read / update;
            debugLog("Read/Update ratio: " + readUpdateRatio);
            
            if(readUpdateRatio > myHighRead)
                switchDatabase(Database.CASSANDRA);
            else if(readUpdateRatio < myLowRead)
                switchDatabase(Database.HBASE);
        }
        
    }

    @Override
    public int read (String table,
                     String key,
                     Set<String> fields,
                     HashMap<String, String> result) {
        recordAndEvaluateSwitch(Operation.READ);
        debugLog("Primary READ");
        return myCurrent.read(table, key, fields, result);
    }


    @Override
    public int scan (String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, String>> result) {
        recordAndEvaluateSwitch(Operation.SCAN);
        debugLog("Primary SCAN");
        return myCurrent.scan(table, startkey, recordcount, fields, result);
    }


    @Override
    public int update (String table, String key, HashMap<String, String> values) {
        recordAndEvaluateSwitch(Operation.UPDATE);
        
        myUpdater.addOperation(new Update(myOther, table, key, values));
        debugLog("Primary UPDATE");
        return myCurrent.update(table, key, values);
    }


    @Override
    public int insert (String table, String key, HashMap<String, String> values) {
        recordAndEvaluateSwitch(Operation.INSERT);
        
        myUpdater.addOperation(new Insert(myOther, table, key, values));
        debugLog("Primary INSERT");
        return myCurrent.insert(table, key, values);
    }


    @Override
    public int delete (String table, String key) {
        recordAndEvaluateSwitch(Operation.DELETE);
        
        myUpdater.addOperation(new Delete(myOther, table, key));
        debugLog("Primary DELETE");
        return myCurrent.delete(table, key);
    }

}
