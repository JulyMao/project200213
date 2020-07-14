package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-06-22 18:13
 */
public class HBase01_DDL2 {
    //编写统一代码
    private static Connection connection;
    private static Admin admin;
    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void stop() throws IOException {
        admin.close();
        connection.close();
    }

    public static void createNS(String ns) throws IOException {
        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();
        admin.createNamespace(build);
        stop();
    }

    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName,String... cfs) throws IOException {
        if (cfs.length <= 0) {
            System.out.println("请输入列族信息！！！");
            return;
        }

        if (isTableExist(tableName)) {
            System.out.println(tableName + "该表已存在！");
            return;
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String cf: cfs){
            ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            tableDescriptorBuilder.setColumnFamily(build);
        }
        tableDescriptorBuilder.setCoprocessor("com.atguigu.HBase03_Coprocessor");
        TableDescriptor build = tableDescriptorBuilder.build();
        admin.createTable(build);
        stop();
    }

    public static void deleteTable(String tableName) throws IOException {
        if (!isTableExist(tableName)){
            System.out.println("表不存在");
            return;
        }

        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        stop();

    }

    public static void main(String[] args) throws IOException {
//        createNS("maow");
//        System.out.println(isTableExist("maow:stu7"));
//        createTable("maow:stu7","info1","info2");
//        System.out.println(isTableExist("maow:stu7"));
//        deleteTable("maow:stu7");
        createTable("stu7","info1","info2");
    }

}
