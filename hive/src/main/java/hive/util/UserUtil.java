package hive.util;

import hdfs.util.HdfsUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 用户操作工具类
 */
public class UserUtil {
    private static final Logger log = Logger.getLogger(UserUtil.class);

    /**
     * 创建新用户
     * @param userName
     * @return boolean
     */
    public boolean createUser(String userName) {
        HdfsUtil hdfsUtil = new HdfsUtil();
        String uri = "/user/ljbao"+userName;
        try {
            hdfsUtil.mkdir(uri);
            hdfsUtil.modifyOwner(new Path(uri),userName,"public");
            log.info("用户创建成功！");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            log.info("用户创建失败！");
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
    }
}
