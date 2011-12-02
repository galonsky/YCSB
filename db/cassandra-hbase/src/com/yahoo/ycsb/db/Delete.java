package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;

public class Delete implements Runnable {
    
    private DB myClient;
    private String myTable;
    private String myKey;
    private int result;
    
    public Delete (DB client, String table, String key) {
        myClient = client;
        myTable = table;
        myKey = key;
    }

    @Override
    public void run () {
        result = myClient.delete(myTable, myKey);
    }
    
    public int getResult() {
        return result;
    }

}
