package hive.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveClient3 {
    protected final Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
    IMetaStoreClient client;

    public HiveClient3() {
        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.addResource("hive-site.xml");
            client = RetryingMetaStoreClient.getProxy(hiveConf,true);
        } catch (MetaException ex) {
            logger.error(ex.getMessage());
        }
    }

    public static void main(String[] args) throws TException {
        HiveClient3 hiveClient3 = new HiveClient3();
        hiveClient3.createTable();
    }
    /**
     * String tableName,
     * String dbName,
     * String owner,
     * int createTime,
     * int lastAccessTime,
     * int retention,
     * StorageDescriptor sd,
     * List<FieldSchema> partitionKeys,
     * Map<String, String> parameters,
     * String viewOriginalText,
     * String viewExpandedText,
     * String tableType
     *
     * @return
     */
    public boolean createTable(){
        Table tb = new Table();
        Type typ1 = new Type();
        typ1.setName("Person");
        typ1.setFields(new ArrayList<FieldSchema>(2));
        typ1.getFields().add(
                new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
        typ1.getFields().add(
                new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(typ1.getFields());
        sd.setNumBuckets(1);
        sd.setParameters(new HashMap<String, String>());
        sd.getParameters().put("test_param_1", "Use this for comments etc");
        sd.setBucketCols(new ArrayList<String>(2));
        sd.getBucketCols().add("name");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        sd.setInputFormat("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        sd.setSerdeInfo(new SerDeInfo());
//        sd.getSerdeInfo().setName(tb.getTableName());
//        sd.getSerdeInfo().setParameters(new HashMap<String, String>());
//        sd.getSerdeInfo().getParameters().put(
//                org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
//        sd.getSerdeInfo().setSerializationLib(
//                org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
//        sd.setLocation("/apps/hive/warehouse/db2.db/tb5");

        tb.setPartitionKeys(new ArrayList<FieldSchema>());
        tb.setTableName("tb6");
        tb.setDbName("db2");
        tb.setSd(sd);
        try {
            client.createTable(tb);
            return true;
        } catch (TException e) {
            e.printStackTrace();
        }
        return false;
    }



    /**
     * 创建数据库
     * @param dbName
     * @param description
     * @param locationUrl
     * @param properties
     * @return
     * @throws TException
     */
    public boolean createDatabase(String dbName, String description,String locationUrl, Map<String,String> properties) throws TException {
        Database db = new Database(dbName,description,locationUrl,properties);
        client.createDatabase(db);
        return true;
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

}
