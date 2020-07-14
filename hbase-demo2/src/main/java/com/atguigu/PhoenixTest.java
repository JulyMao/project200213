package com.atguigu;

import org.apache.phoenix.queryserver.client.ThinClientUtil;

import java.sql.*;

/**
 * @author maow
 * @create 2020-06-23 15:43
 */
public class PhoenixTest {
    public static void main(String[] args) throws SQLException {
        String connectionUrl = ThinClientUtil.getConnectionUrl("hadoop102", 8765);
        System.out.println(connectionUrl);
        Connection connection = DriverManager.getConnection(connectionUrl);
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getString(1) + "\t" + resultSet.getString(2));
        }

        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
