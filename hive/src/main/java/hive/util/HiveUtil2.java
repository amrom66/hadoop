package hive.util;

import com.google.inject.internal.asm.$Attribute;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用原生api实现操作
 * 原理：访问元数据库metastore
 */
public class HiveUtil2 {
    private static final Logger logger = org.apache.log4j.Logger.getLogger(HiveClient.class);
    private static final String url = "jdbc:hive2://172.16.23.190:10000";
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";

    private static IMetaStoreClient client;

    private static HiveMetaStoreClient hiveMetaStoreClient;

    public static void main(String[] args) throws TException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource("hive-site.xml");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        hiveMetaStoreClient.close();
    }


    /**
     * 获取所有数据库
     * @return 数据库集合
     */
    public List<String> getAllDatabases() {
        List<String> databases = null;
        try {
            databases = client.getAllDatabases();
        } catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return databases;
    }

    /**
     * 获取单个数据库
     * @param db 数据库名称
     * @return
     */
    public Database getDatabase(String db) {
        Database database = null;
        try {
            database = client.getDatabase(db);
        } catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return database;
    }

    /**
     *  Map dbproperties = new HashMap();
     *      dbproperties.put("creator", "linjb");
     *      dbproperties.put("data", "2019-12-31");
     * Database database = new Database("employee6","meiyou miaoshu ",null, null);
     * 数据库名 描述 自定义地址（"/user/ljbao/"） 参数
     * @param dbName 数据库名称
     * @param description 数据库描述
     * @param locationUri 指定库的地址
     * @param dbproperties 库的一些属性，如创建人，创建时间
     * @return
     */
    public boolean createDataBase(String dbName, String description, String locationUri, Map<String,String> dbproperties){
        Database database = new Database(dbName, description, locationUri, dbproperties);
        try {
            client.createDatabase(database);
            return true;
        } catch (TException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean createTable(){

        return false;
    }

    /**
     * 获得约束信息集合
     * @param db 数据库
     * @param table 数据表
     * @return
     */
    public List<FieldSchema> getSchema(String db, String table) {
        List<FieldSchema> schema = null;
        try {
            schema = client.getSchema(db, table);
        } catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return schema;
    }

    /**
     * 获取tables集合
     * @param db 指定库名
     * @return
     */
    public List<String> getAllTables(String db) {
        List<String> tables = null;
        try {
            tables = client.getAllTables(db);
        } catch (TException ex) {
            logger.error(ex.getMessage());
        }
        return tables;
    }

    /**
     * 获取指定表的hdfs存储位置
     * @param db 库名
     * @param table 表名
     * @return
     */
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
