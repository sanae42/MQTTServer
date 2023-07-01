package org.example;

import java.sql.*;

public class mysql {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
//        1.加载驱动
        Class.forName("com.mysql.cj.jdbc.Driver");//固定写法
//        2.用户信息和url
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=true";
        String username="root";
        String password="GPF200017";
//        3.连接成功，数据库对象 Connection
        Connection connection = DriverManager.getConnection(url,username,password);
//        4.执行SQL对象Statement，执行SQL的对象
        Statement statement = connection.createStatement();
//        5.执行SQL的对象去执行SQL，返回结果集
        String sql = "SELECT *FROM USER;";
        ResultSet resultSet = statement.executeQuery(sql);
        while(resultSet.next()){
            System.out.println("id="+resultSet.getString("id"));
            System.out.println("name="+resultSet.getString("name"));
            System.out.println("password="+resultSet.getString("password"));
        }
//        6.释放连接
        resultSet.close();
        statement.close();
        connection.close();
    }
}