package com.yahoo.ycsb.db;

import java.util.LinkedList;
import java.util.Queue;

public class SecondaryUpdater implements Runnable {
    
    Queue<Runnable> myPendingWrites;
    boolean running;
    boolean debug;
    
    public SecondaryUpdater(boolean d) {
        myPendingWrites = new LinkedList<Runnable>();
        running = true;
        debug = d;
    }
    
    public void addOperation(Runnable r) {
        myPendingWrites.add(r);
    }
    
    public void destroy() {
        running = false;
    }
    
    public boolean isBusy() {
        return !myPendingWrites.isEmpty();
    }

    @Override
    public void run () {
        while(running) {
            if(!myPendingWrites.isEmpty()) {
                Runnable r = myPendingWrites.poll();
                if(debug)
                    System.out.println("Secondary write: " + r.toString());
                
                r.run();
            }
        }
    }

}
