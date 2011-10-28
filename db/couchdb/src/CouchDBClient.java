import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import net.sf.json.JSONObject;
import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;
import com.fourspaces.couchdb.View;
import com.fourspaces.couchdb.ViewResults;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;


public class CouchDBClient extends DB {
    
    private final static String HOST = "ubuntu";
    private Session mySession;
    
    @Override
    public void init() throws DBException {
        mySession = new Session(HOST, 5984);
    }

    @Override
    public int read (String table,
                     String key,
                     Set<String> fields,
                     HashMap<String, String> result) {
        try {
            Database db = mySession.getDatabase(table);
            Document doc = db.getDocument(key);
            docToMap(doc, result);
            return 0;
        }
        catch (IOException e) {
            return -1;
        }
    }

    @Override
    public int scan (String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, String>> result) {
        Database db = mySession.getDatabase(table);
        View v = new View("_all_docs");
        //v.setWithDocs(new Boolean(true));
        // setWithDocs is broken in current couchdb4j. 
        // Patch submitted here: https://github.com/mbreese/couchdb4j/pull/1
        // Using hack below in the meantime.
        String start = "%22" + startkey + "%22&include_docs=true";
        
        v.setLimit(new Integer(recordcount));
        v.setStartKey(start);

        List<Document> results = db.view(v).getResults();
        for (Document doc : results) {
            JSONObject json = doc.getJSONObject("doc");
            HashMap<String, String> map = new HashMap<String, String>();
            docToMap(json, map);
            result.add(map);
        }
        return 0;
    }

    @Override
    public int update (String table, String key, HashMap<String, String> values) {
        try {
            Database db = mySession.getDatabase(table);
            Document doc = db.getDocument(key);
            for(String k : values.keySet()) {
                doc.put(k, values.get(k));
            }
            db.saveDocument(doc);
            return 0;
        }
        catch (IOException e) {
            return -1;
        }
    }

    @Override
    public int insert (String table, String key, HashMap<String, String> values) {
        try {
            Database db = mySession.getDatabase(table);
            Document doc = new Document();
            for(String k : values.keySet()) {
                doc.put(k, values.get(k));
            }
            doc.put("_id", key);
            db.saveDocument(doc);
            return 0;
        }
        catch (IOException e) {
            return -1;
        }
    }

    @Override
    public int delete (String table, String key) {
        try {
            Database db = mySession.getDatabase(table);
            Document doc = db.getDocument(key);
            db.deleteDocument(doc);
            return 0;
        }
        catch (IOException e) {
            return -1;
        }
    }
    
    private void docToMap(Map doc, HashMap<String, String> map) {
        for(String k : (Set<String>) doc.keySet()) {
            map.put(k, (String) doc.get(k)); 
        }
    }
    
    public static void main(String[] args) throws DBException {
        CouchDBClient couch = new CouchDBClient();
        couch.init();
        Vector<HashMap<String,String>> results = new Vector<HashMap<String,String>>();
        couch.scan("test", "key", 5, null, results);
        System.out.println(results.get(0).get("poop"));
        
        //HashMap<String, String> map = new HashMap<String, String>();
        //couch.read("test", "key", null, map);
        //System.out.println(map.get("poop"));
        //map.put("name", "joe");
        //map.put("age", "21");
        //couch.insert("test", "galonsky", map);
        //couch.update("test", "galonsky", map);
        //couch.delete("test", "galonsky");
        //couch.scan("test", "key", 5, null, null);
        
        
    }

}
