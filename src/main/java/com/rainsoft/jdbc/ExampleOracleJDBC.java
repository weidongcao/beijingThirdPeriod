package com.rainsoft.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.rainsoft.conf.ConfigurationManager;

/**
 * Created by Administrator on 2017-06-13.
 */
public class ExampleOracleJDBC {
    //数据库连接驱动
    private static String driver = ConfigurationManager.getProperty("oracle.driver");
    //数据库连接地址
    private static String url = ConfigurationManager.getProperty("oracle.url");
    //数据库连接用户名
    private static String username = ConfigurationManager.getProperty("oracle.username");
    //数据库连接密码
    private static String password = ConfigurationManager.getProperty("oracle.password");

    public static void main(String[] args) {

        System.out.println("数据库连接信息：");
        System.out.println("driver = " + driver);
        System.out.println("url = " + url);
        System.out.println("username = " + username);
        System.out.println("password = " + password);
        // 创建一个数据库连接
        Connection con = null;
        // 创建预编译语句对象，一般都是用这个而不用Statement
        PreparedStatement pre = null;
        // 创建一个结果集对象
        ResultSet result = null;
        try {
            // 加载Oracle驱动程序
            Class.forName(driver);
            System.out.println("开始尝试连接数据库！");
            // 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
            // 获取连接
            con = DriverManager.getConnection(url, username, password);
            System.out.println("连接成功！");
            // 预编译语句，“？”代表参数
            String sql = "select * from REG_CONTENT_HTTP where rownum < 3";
            // 实例化预编译语句
            pre = con.prepareStatement(sql);
            // 执行查询，注意括号中不需要再加参数
            result = pre.executeQuery();
            while (result.next()) {
                for (int i = 1; i < 28; i++) {
                    System.out.print(result.getString(i) + "\t");
                }
                // 当结果集不为空时
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
                // 注意关闭的顺序，最后使用的最先关闭
                if (result != null)
                    result.close();
                if (pre != null)
                    pre.close();
                if (con != null)
                    con.close();
                System.out.println("数据库连接已关闭！");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
