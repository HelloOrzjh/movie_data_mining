package com.orzjh.movie_data_mining.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/21 16:05
 */
public class HiveJdbcUtils {
    private final static Logger LOG = LoggerFactory.getLogger(HiveJdbcUtils.class);
    private final static String URL = "jdbc:hive2://hadoop001:10000/ml_25m_database";
    private final static String USER = "root";
    private final static String PASSWORD = "root";

    static {
        try {
            // 加载Hive JDBC驱动
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            LOG.info("Load hive driver success.");
        } catch (Exception e) {
            LOG.error("Load hive driver failed, msg is " + e.getMessage());
        }
    }

    // 声明连接对象
    private Connection conn = null;

    // 初始化连接地址
    public HiveJdbcUtils() {
        try {
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            LOG.info("Get connection success.");
        } catch (Exception e) {
            LOG.error("Get connection failed, msg is " + e.getMessage());
        }
    }

    public Connection getConnection() {
        return conn;
    }

    public ResultSet executeQuery(String hql) throws SQLException {
        Statement stmt = null;
        ResultSet res = null;

        if (conn != null) {
            LOG.info("HQL[" + hql + "]");
            stmt = conn.createStatement();
            res = stmt.executeQuery(hql);
        } else {
            LOG.error("Object [conn] is null");
        }

        return res;
    }

    public void executeUpdate(String hql) throws SQLException {
        Statement stmt = null;
        if (conn != null) {
            LOG.info("HQL[" + hql + "]");
            stmt = conn.createStatement();
            int affect_rows = stmt.executeUpdate(hql);
            LOG.info(String.valueOf(affect_rows) + " rows affected");
        } else {
            LOG.error("Object [conn] is null");
        }
    }

    public void close() {
        try {
            if (!conn.isClosed()) {
                conn.close();
            }
            LOG.info("Close connect success.");
        } catch (Exception e) {
            LOG.error("Close connect failed, msg is " + e.getMessage());
        }
    }
}