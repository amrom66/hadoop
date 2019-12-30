package com.hive;

import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 *  * Created by Ganymede
 *  * 限制了超级管理员权限，普通用户不能授权、建库、建表等操作
 *  项目无关，只做hive配置使用，使用方法：https://linjinbao66.github.io/2019/2019-12-29-hive%E8%B8%A9%E5%9D%91%E7%BA%AA%E5%AE%9E/
 *   */
public class HiveAdmin extends AbstractSemanticAnalyzerHook {
    private static String[] admin = {"root", "hadoop", "hive", "ljbao"};  //配置Hive管理员

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
                              ASTNode ast) throws SemanticException {
        switch (ast.getToken().getType()) {
            case HiveParser.TOK_CREATEDATABASE:
            case HiveParser.TOK_DROPDATABASE:
            case HiveParser.TOK_CREATEROLE:
            case HiveParser.TOK_DROPROLE:
            case HiveParser.TOK_GRANT:
            case HiveParser.TOK_REVOKE:
            case HiveParser.TOK_GRANT_ROLE:
            case HiveParser.TOK_REVOKE_ROLE:
            case HiveParser.TOK_CREATETABLE:
                String userName = null;
                if (SessionState.get() != null
                        && SessionState.get().getAuthenticator() != null) {
                    userName = SessionState.get().getAuthenticator().getUserName();
                }
                if (!admin[0].equalsIgnoreCase(userName)
                        && !admin[1].equalsIgnoreCase(userName) && !admin[2].equalsIgnoreCase(userName)&& !admin[3].equalsIgnoreCase(userName)) {
                    throw new SemanticException(userName
                            + " can't use ADMIN options, except " + admin[0] + "," + admin[1] + ","
                            + admin[2] + admin[3] + ".");
                }
                break;
            default:
                break;
        }
        return ast;
    }


    public static void main(String[] args) throws SemanticException {
        String[] admin = {"admin", "root"};
        String userName = "root1";
        for (String tmp : admin) {
            System.out.println(tmp);
            if (!admin[0].equalsIgnoreCase(userName) && !admin[1].equalsIgnoreCase(userName)) {
                throw new SemanticException(userName
                        + " can't use ADMIN options, except " + admin[0] + ","
                        + admin[1] + ".");
            }
        }
    }
}
