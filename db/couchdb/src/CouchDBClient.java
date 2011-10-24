import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import com.yahoo.ycsb.DB;


public class CouchDBClient extends DB {

    @Override
    public int read (String table,
                     String key,
                     Set<String> fields,
                     HashMap<String, String> result) {
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int insert (String table, String key, HashMap<String, String> values) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int delete (String table, String key) {
        // TODO Auto-generated method stub
        return 0;
    }

}
