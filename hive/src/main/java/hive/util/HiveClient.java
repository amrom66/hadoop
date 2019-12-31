package hive.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import java.util.List;

public class HiveClient {
    protected final Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
    static IMetaStoreClient client;

    public HiveClient() {
        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.addResource("C:\\Users\\User\\IdeaProjects\\hadoop\\hive\\src\\main\\resources\\hive-site.xml");
            client = RetryingMetaStoreClient.getProxy(hiveConf,true);
        } catch (MetaException ex) {
            logger.error(ex.getMessage());
        }
    }

    public List<String> getAllDatabases() {
        List<String> databases = null;
        try {
            databases = client.getAllDatabases();
        } catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return databases;
    }

    public Database getDatabase(String db) {
        Database database = null;
        try {
            database = client.getDatabase(db);
        } catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return database;
    }

    public List<FieldSchema> getSchema(String db, String table) {
        List<FieldSchema> schema = null;
        try {
            schema = client.getSchema(db, table);
        } catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return schema;
    }

    public List<String> getAllTables(String db) {
        List<String> tables = null;
        try {
            tables = client.getAllTables(db);
        } catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return tables;
    }

    public String getLocation(String db, String table) {
        String location = null;
        try {
            location = client.getTable(db, table).getSd().getLocation();
        }catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return location;
    }

    public static void main(String[] args) throws TException {
        HiveClient hiveClient = new HiveClient();
        Database database = new Database("employee6","meiyou miaoshu ",null, null);
        client.close();
        Role role = new Role("myrole5",1577772926,"ljbao");
        client.create_role(role);
        List l = hiveClient.getAllDatabases();
        System.out.println(l);
        List roles = client.listRoleNames();
        System.out.println(roles);
    }

    public void test(){
        Database database = new Database();

    }

}