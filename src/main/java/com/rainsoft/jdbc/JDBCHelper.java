package com.rainsoft.jdbc;


import com.rainsoft.conf.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * JDBC辅助组件
 * 获取数据库连接，执行增删改查
 * Created by caoweidong on 2017/2/5.
 *
 * @author caoweidong
 */
public class JDBCHelper {
    private static final Logger logger = LoggerFactory.getLogger(JDBCHelper.class);
    private static final String driver = ConfigurationManager.getProperty("oracle.driver");
    private static final String url = ConfigurationManager.getProperty("oracle.url");
    private static final String username = ConfigurationManager.getProperty("oracle.username");
    private static final String password = ConfigurationManager.getProperty("oracle.password");

    //加载驱动
    static {
        try {
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 实现JDBCHelper的单例化
    private static JDBCHelper instance = null;


    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    //数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    /**
     * 私有化构造方法
     */
    private JDBCHelper() {
        // 第一步：获取数据库连接池的大小，就是说数据库连接池中要放多少个数据库连接
        int datasourceSize = 5;

        //创建指定数量的数据库连接，并放入数据库连接池中
        for (int i = 0; i < datasourceSize; i++) {

            try {
                Connection conn = DriverManager.getConnection(url, username, password);
                datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 提供获取数据库连接的方法
     */
    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 以SQL传参的方式执行增删改SQL语句
     *
     * @param sql    　SQL语句
     * @param params 　参数
     * @return
     */
    public int executeUpdateByParams(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            rtn = pstmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 以完整SQL的方式执行一条增删改语句
     *
     * @param sql 　SQL语句
     * @return
     */
    public int executeUpdateBySql(String sql) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            rtn = pstmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 以SQL传参的方式执行查询SQL语句
     *
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQueryByParams(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            rs = pstmt.executeQuery();

            callback.process(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 以完整SQL的方式执行查询SQL语句
     *
     * @param sql
     * @param callback
     */
    public void executeQueryBySql(String sql, QueryCallback callback) {
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            st = conn.createStatement();
            rs = st.executeQuery(sql);
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 以SQL传参的形式批量执行sql语句
     *
     * @param sql        SQL语句模板
     * @param paramsList SQL参数
     * @return
     */
    public int[] executeBatchByParams(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();

            //使用Connection对象，取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);

            //使用PreparedStatement.addBatch()方法加入指的sql参数
            for (Object[] params : paramsList) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
                pstmt.addBatch();
            }

            //使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            //使用Connection对象指批量的SQL语句
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 参完整SQL语句的形式执行批量操作
     *
     * @param sqlList 完整SQL
     * @return
     */
    public int[] executeBatchBySql(List<String> sqlList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();

            //使用Connection对象，取消自动提交
            conn.setAutoCommit(false);
            pstmt = (PreparedStatement) conn.createStatement();

            for (String sql : sqlList) {
                pstmt.addBatch(sql);
            }

            //使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            //使用Connection对象指批量的SQL语句
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 静态内部类:查询回调接口
     *
     * @author caoweidong
     */
    public static interface QueryCallback {
        /**
         * 处理查询结果
         *
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }

    public static void main(String[] args) {

    }
}
