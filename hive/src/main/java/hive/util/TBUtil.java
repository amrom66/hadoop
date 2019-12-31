package hive.util;

import hive.entity.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据表工具类
 */
public class TBUtil {

    private static Connection connection = HiveUtil.getConn();
    private static final Logger logger = Logger.getLogger(TBUtil.class);

    /**
     * 创建数据表 传入封装的table对象
     *  List<Field> list = new ArrayList();
     *  Field field1 = new Field("name", "string", "'employee names'");
     *  Field field2 = new Field("salary", "float", "'employee salary'");
     *  list.add(field1);
     *  list.add(field2);
     *
     *  Table table = new Table("test", list, "table comment",
     *                 "('creator'='me','created_at'='2019-12-29 10:00:00') ", "'/user/ljbao/employees'");
     *  tbUtil.createTB(table);
     *   Table table = new Table("test", list, null, null,null);
     * @param table
     * @return
     */
    public boolean createTB(Table table){
        String sql = "create table if not exists " +table.toString();
        System.out.println("sql = " + sql);
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建分区表----待实现
     * @param table
     * @return
     */
    public boolean createTB(Table table, Partition partition){

        return false;
    }

    /**
     * 从一张已有的表创建新表
     * @param srcName 源表名
     * @param destName 目的表名
     * @return
     */
    public boolean createTB(String srcName, String destName){
        String sql = "create table if not exists " + destName + " like " +srcName;
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 显示当前数据库下的所有表
     * @return
     */
    public List showTables(){
        List<String> list = new ArrayList();
        String sql = "show tables";
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            while (resultSet.next()){
                list.add(resultSet.getString(1));
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 显示指定数据库下的所有表
     * @param dbName
     * @return
     */
    public List showTables(String dbName){
        List<String> list = new ArrayList();
        String sql = "show tables in " + dbName;
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            while (resultSet.next()){
                list.add(resultSet.getString(1));
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 查看表的信息
     * 可选参数 formatted extended
     * tbUtil.describeTable("ljbao.testa")
     * @param tbName
     * @return
     */
    public List describeTable(String tbName){
        String sql = "describe " + tbName;
        List<String> list = new ArrayList();
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            System.out.println(resultSet.getFetchSize());
            while (resultSet.next()){
                list.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }




    public static void main(String[] args) throws HiveException, TException {
        TBUtil tbUtil = new TBUtil();
        System.out.println( );

        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource("C:\\Users\\User\\IdeaProjects\\hadoop\\hive\\src\\main\\resources\\hive-site.xml");
        System.out.println(hiveConf.getAllProperties());

        IMetaStoreClient client = RetryingMetaStoreClient.getProxy(hiveConf,true);

        List list = client.getAllDatabases();
        System.out.println(list);



    }

}
