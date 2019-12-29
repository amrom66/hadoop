package hive.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * hive连接工具类
 */
public class HiveUtil {
    private static final Logger log = LoggerFactory.getLogger(HiveUtil.class);
    private static final String url = "jdbc:hive2://192.168.220.130:10000";
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String user = "ljbao" ;
    private static String password = "" ;
    private static Connection conn = null;
    private static String sql = "";
    private static ResultSet res;

    /**
     * 初始化连接
     */
    static {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
            log.info("连接已经建立");
            log.info("连接的url = " + url);
            log.info("当前用户 = " + user);
        } catch (Exception e) {
            e.printStackTrace();
            conn = null;
        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        HiveUtil hiveUtil = new HiveUtil();
    }

    /**
     * 创建角色
     * @param roleName
     * @throws SQLException
     */
    public void createRole(String roleName) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("set role admin");     //切换成admin角色
        stmt.execute("create role " + roleName);
    }

    /**
     * 建库语句
     * @param databaseName
     */
    public void createDatabase(String databaseName) throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("create database " + databaseName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 切换数据库
     * @param databaseName
     */
    public void changeDatabase(String databaseName) {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("use " + databaseName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询制指定条数数据
     * @param tableName
     * @return list
     */
    public List<String> tableMsg(String tableName, int limit){
        List<String> list = new ArrayList<String>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from "+ tableName +" limit " + limit);
            while(rs.next()) {
                String a = "";
                for(int i = 1; i <= limit; i++) {
                    a += rs.getString(i) + "\t";
                }
                list.add(a);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }


    /**
     * 执行regular hive query操作
     * @param tableName
     * @throws SQLException
     * @return
     */
    private void countData(String tableName) throws SQLException {
        Statement stmt = conn.createStatement();
        sql = "select count(1) from " + tableName;
        res = stmt.executeQuery(sql);
        System.out.println("执行“regular hive query”运行结果:");
        while (res.next()) {
            System.out.println("count ------>" + res.getString(1));
        }
    }

    /**
     * select * 操作
     * @param stmt
     * @param tableName
     * @throws SQLException
     */

    private void selectData(Statement stmt, String tableName)
            throws SQLException {
        sql = "select * from " + tableName;
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        System.out.println("执行 select * query 运行结果:");
        while (res.next()) {
            System.out.println(res.getInt(1) + "\t" + res.getString(2));
        }
    }

    /**
     * 数据导入
     * @param stmt
     * @param tableName
     * @throws SQLException
     */
    private void loadData(Statement stmt, String tableName)
            throws SQLException {
        String filepath = "user.txt";
        sql = "load data local inpath '" + filepath + "' into table "
                + tableName;
        System.out.println("Running:" + sql);
        stmt.execute(sql);
    }

    /**
     * 查看建表语句
     * @param stmt
     * @param tableName
     * @throws SQLException
     */
    private void describeTables(Statement stmt, String tableName)
            throws SQLException {
        sql = "describe " + tableName;
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        System.out.println("执行 describe table 运行结果:");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }

    /**
     * 列出所有表
     * @param stmt
     * @param tableName
     * @throws SQLException
     */
    private void showTables(Statement stmt, String tableName)
            throws SQLException {
        sql = "show tables '" + tableName + "'";
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        System.out.println("执行 show tables 运行结果:");
        if (res.next()) {
            System.out.println(res.getString(1));
        }
    }

    /**
     * 建表不指定数据库
     * @param stmt
     * @param tableName
     * @throws SQLException
     */
    private boolean createTable(Statement stmt, String tableName)
            throws SQLException {
        sql = "create table "
                + tableName
                + " (key int, value string)  row format delimited fields terminated by '\t'";
        return stmt.execute(sql);
    }

    private void createTable(Statement stmt, String databaseName, String tableName){

    }

    /**
     * 删除表
     * @param stmt
     * @param tableName
     * @return
     * @throws SQLException
     */
    private boolean dropTable(Statement stmt, String tableName) throws SQLException {
        sql = "drop table  " + tableName;
        return stmt.execute(sql);
    }

    /**
     * 获取自定义sql返回结果
     * @param sql
     * @param stmt
     * @return
     */
    public List<String> getResultData(Statement stmt,String sql){
        List<String> list = new ArrayList<String>();
        // 生成一个对于当前流程唯一的中间表名称
        // 如果流程会反复执行则先删除该表再创建
        String tableName = "data_flow";
        sql = "create table " + tableName + " as " + sql;
        try {
            stmt.execute(sql);
            int tableSize = stmt.executeQuery("select count(1) from " + tableName).getInt(1);
            ResultSet rs = stmt.executeQuery("select * from " + tableName);
            while(rs.next()) {
                String a = "";
                for(int i = 1; i <= tableSize; i++) {
                    a += rs.getString(i) + "\t";
                }
                list.add(a);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}
