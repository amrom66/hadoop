import hdfs.util.ExecuteShellUtil;
import org.junit.Test;

public class TestShell {

    @Test
    public void test01() throws Exception {
        ExecuteShellUtil executeShellUtil = new ExecuteShellUtil();
        executeShellUtil.init("192.168.220.130",22, "ljbao", "369369");

        String result = executeShellUtil.execCmd("echo $PATH");
        System.out.println(result);
    }
}
