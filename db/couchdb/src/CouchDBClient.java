import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;
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
            for(String k : (Set<String>) doc.keySet()) {
                result.put(k, doc.getString(k)); 
            }
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
        // TODO Auto-generated method stub
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
    
    public static void main(String[] args) throws DBException {
        CouchDBClient couch = new CouchDBClient();
        couch.init();
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("name", "joe");
        map.put("age", "21");
        //couch.insert("test", "galonsky", map);
        //couch.update("test", "galonsky", map);
        couch.delete("test", "galonsky");
        
        
    }

}
