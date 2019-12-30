package hive.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库操作工具类
 */

public class DBUtil {

    private static Connection connection = HiveUtil.getConn();

    /**
     * 创建数据库
     * @param dbName 数据库名称
     * @return
     */
    public boolean createDB(String dbName){
        String sql = "create database " + dbName;
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建数据库-指定文件存放位置
     * @param dbName
     * @param path
     * @return
     */
    public boolean createDB(String dbName, String path){
        String sql = "create database if exists " + dbName + " location " + path;
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 待优化！！！
     * 创建数据库-指定文件存放位置-添加数据库属性描述
     * @param dbName
     * @param path
     * @param dbProperties with diproperties ('creator' = 'ljbao', 'date' = '2019-12-30')
     * @return
     */
    public boolean createDB(String dbName, String path, String dbProperties){
        String sql = "create database if exists " + dbName + " location " + path + " with dbproperties" + dbProperties;
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除数据库--数据库不含有表的时候调用
     * @param dbName
     * @return
     */

    public boolean dropDB(String dbName){
        String sql = "drop database if exists" + dbName;
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 含有表的数据库删除
     * @param dbName
     * @param isContainsTabs
     * @return
     */

    public boolean dropDB(String dbName, boolean isContainsTabs){
        String sql = "drop database if exists" + dbName + " cascade";
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 修改dbproperties
     * @param dbName
     * @param dbpropertie diproperties ('creator' = 'ljbao', 'date' = '2019-12-30')
     * @return
     */
    public boolean alterDB(String dbName, String dbpropertie){
        String sql = "alter database " + dbName +" set dbproperties " + dbpropertie;
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 显示所有数据库
     */
    public List getAllDBs(){
        List<String> list = new ArrayList();
        try {
            ResultSet resultSet = connection.createStatement().executeQuery("show databases");
            while (resultSet.next()){
                list.add(resultSet.getString("database_name"));
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 切换当前数据库
     * @param
     */
    public void switchDB(String dbName){
        String sql = "use " + dbName;
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 显示当前数据库
     * @return
     */
    public String getCurrentDB(){
        String sql = "select current_database()";
        String currentName = null;
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            while (resultSet.next()){
                currentName = resultSet.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return currentName;
    }


    public static void main(String[] args) {
        DBUtil DBUtil = new DBUtil();
        System.out.println(DBUtil.getCurrentDB());
    }

}
