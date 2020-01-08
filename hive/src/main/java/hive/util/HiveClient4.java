package hive.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class HiveClient4 {
    protected final Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
    static IMetaStoreClient client;

    public HiveClient4() {
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
//        HiveClient hiveClient = new HiveClient();
//        Database database = new Database("employee6","meiyou miaoshu ",null, null);
//        client.close();
//        Role role = new Role("myrole5",1577772926,"ljbao");
//        client.create_role(role);
//        List l = hiveClient.getAllDatabases();
//        System.out.println(l);
//        List roles = client.listRoleNames();
//        System.out.println(roles);
//        Table table = new Table();
//        table.setTableName("employees7");
//        table.setDbName("ljbao");
//        table.setOwner("ljbao");
//        Map map = new HashMap() ;
//        map.put("id", "int");
//        table.setParameters(map);

//        Table table = client.getTable("ljbao","employees2");
        HiveClient4 hiveClient = new HiveClient4();
        List list = client.getAllTables("emp");
        System.out.println(list);
//        client.createTable(table);
//        client.close();


    }

    /**
     *  String tableName,
     * 	String dbName,
     * 	String owner,
     * 	int createTime,
     * 	int lastAccessTime,
     * 	int retention,
     * 	StorageDescriptor sd,
     * 	List<FieldSchema> partitionKeys,
     * 	Map<String, String> parameters,
     * 	String viewOriginalText,
     * 	String viewExpandedText,
     * 	String tableType
     */
    /**
     * create table if not exists ljbao.employees (
     * name string comment 'employee name',
     * salary float comment 'salary',
     * deductions map<String, float> comment 'keys are name and values are per...'
     * )
     * tblproperties ('creator'='me','created_at'='2019-12-29 10:00:00')
     * location '/user/ljbao/employees';
     */
    @Test
    public void test01() throws TException {
        Table table = new Table();
        table.setDbName("ljbao");
        table.setTableName("employees9");
        Type typ1 = new Type();
        typ1.setName("Person");
        typ1.setFields(new ArrayList<FieldSchema>(2));
        typ1.getFields().add(
                new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
        typ1.getFields().add(
                new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(typ1.getFields());
        table.setSd(sd);
        client.createTable(table);

    }

}