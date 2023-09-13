package org.example;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * @Title：Database.java
 * @Description: A Java class related to database operations such as adding, deleting, modifying, and querying
 * @author PerCheung https://blog.csdn.net/weixin_43982359/article/details/119854500
 */
public class Database {
    //Database connect information
    private static String driverName = "com.mysql.cj.jdbc.Driver";
    private static String url = "jdbc:mysql://localhost:3306/IoT?useUnicode=true&characterEncoding=utf8&useSSL=true";
    private static String username = "root";
    private static String password = "GPF200017";
    //jdbc object
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    /**
     * Connect to the database
     */
    public void getConnection() {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Update operation: add, delete, and modify
     */
    public int update(String sql, Object[] objs) {
        int i = 0;
        try {
            getConnection();
            //Create SQL object
            preparedStatement = connection.prepareStatement(sql);
            for (int j = 0; j < objs.length; j++) {
                preparedStatement.setObject(j + 1, objs[j]);
            }
            //Execute SQL and return the number of changed rows
            i = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    /**
     * query operation
     */
    public ResultSet select(String sql, Object[] objs) {
        try {
            getConnection();
            //Create SQL object
            preparedStatement = connection.prepareStatement(sql);
            for (int j = 0; j < objs.length; j++) {
                preparedStatement.setObject(j + 1, objs[j]);
            }
            //Execute SQL to return the set found in the query
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    /**
     * close connection
     */
    public void closeConnection() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
