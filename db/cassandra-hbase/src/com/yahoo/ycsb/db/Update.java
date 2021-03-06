package com.yahoo.ycsb.db;

import java.util.HashMap;
import com.yahoo.ycsb.DB;

public class Update implements Runnable {
    
    private DB myClient;
    private String myTable;
    private String myKey;
    private HashMap<String, String> myValues;
    private int result;
    
    public Update(DB client, String table, String key, HashMap<String, String> values) {
        myClient = client;
        myTable = table;
        myKey = key;
        myValues = values;
    }

    @Override
    public void run () {
        result = myClient.update(myTable, myKey, myValues);
    }
    
    public int getResult() {
        return result;
    }

}
