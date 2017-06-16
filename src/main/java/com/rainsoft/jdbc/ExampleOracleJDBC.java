package com.rainsoft.jdbc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by Administrator on 2017-06-13.
 */
public class ExampleOracleJDBC {
    public static void main(String[] args) {
        // 创建一个数据库连接
        Connection con = null;
        // 创建预编译语句对象，一般都是用这个而不用Statement
        PreparedStatement pre = null;
        // 创建一个结果集对象
        ResultSet result = null;
        try {
            // 加载Oracle驱动程序
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("开始尝试连接数据库！");
            // 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
            String url = "jdbc:oracle:thin:@192.168.30.205:1521/rsdb";
            // 用户名,系统默认的账户名
            String user = "gz_inner";
            // 你安装时选设置的密码
            String password = "gz_inner";
            // 获取连接
            con = DriverManager.getConnection(url, user, password);
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
        } catch (Exception e){
            e.printStackTrace();
        } finally{
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
