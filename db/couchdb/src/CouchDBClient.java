import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import org.jcouchdb.db.Database;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;


public class CouchDBClient extends DB {
    
    private final static String HOST = "ubuntu";
    
    @Override
    public void init() throws DBException {
        
    }

    @Override
    public int read (String table,
                     String key,
                     Set<String> fields,
                     HashMap<String, String> result) {
        Database db = new Database(HOST, table);
        
        return 0;
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
        Database db = new Database(HOST, table);
        values.put("_id", key);
        db.updateDocument(values);
        return 0;
    }

    @Override
    public int insert (String table, String key, HashMap<String, String> values) {
        Database db = new Database(HOST, table);
        values.put("_id", key);
        db.createDocument(values);
        return 0;
    }

    @Override
    public int delete (String table, String key) {
        return 0;
    }
    
    public static void main(String[] args) {
        CouchDBClient couch = new CouchDBClient();
        HashMap<String, String> doc = new HashMap<String, String>();
        doc.put("poop", "pee");
        couch.insert("test", "key", doc);
    }

}
