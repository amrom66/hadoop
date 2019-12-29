package hdfs.util;

import com.jcraft.jsch.JSch;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * hdfs操作工具类
 */
public class HdfsUtil {
    private static final Logger log = Logger.getLogger(HdfsUtil.class);
    private static final String url = "hdfs://192.168.220.130:9000";
    private static Configuration conf = null; //配置文件
    private static FileSystem fileSystem = null;
    static {
        try {
            conf = new Configuration();
            conf.set("dfs.umask","002");
            conf.set("fs.defaultFS", url);
            conf.set("dfs.permissions","false");
            System.setProperty("HADOOP_USER_NAME","root");
            fileSystem=FileSystem.get(conf);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 创建文件夹
     * @param dir
     * @throws IOException
     */
    public void mkdir(String dir) throws IOException {
        fileSystem.mkdirs(new Path(dir));
        fileSystem.close();
    }

    /**
     * 改变文件权限
     * @param path 路径 Path p = new Path("/hh");
     * @param userName 新用户名称
     * @param groupName 新用户组
     * @throws IOException
     */
    public void modifyOwner(Path path, String userName, String groupName) throws IOException {
        fileSystem.setOwner(path,userName,groupName);
        fileSystem.close();
    }


    public static void main(String[] args) throws IOException {

    }

}
