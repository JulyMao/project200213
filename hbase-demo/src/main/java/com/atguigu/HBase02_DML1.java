package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author maow
 * @create 2020-06-22 18:46
 */
public class HBase02_DML1 {

    //声明Connection和Admin
    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void stop() throws IOException {
        connection.close();
    }

    //TODO 新增和修改数据
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        //1.获取DML的Table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2.创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //3.给put对象添加数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //4.执行插入数据的操作
        table.put(put);
        //5.释放资源
        table.close();
        stop();
    }
    //TODO 单条查询数据
    public static void getData(String tableName, String rowKey,String cf,String cn) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2.创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //指定列族
//        get.addFamily(Bytes.toBytes(cf));
        //指定列族：列
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //3.查询结果
        Result result = table.get(get);
        //4.解析result
        for (Cell cell : result.rawCells()){
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
            + "\tCN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\tValue:"
                    + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
        //5.释放资源
        table.close();
        stop();
    }

    //TODO 扫描数据Scan
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result result = iterator.next();
            for (Cell cell : result.rawCells()) {
                System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "\tCN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\tValue:"
                        + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        table.close();
        stop();
    }

    //TODO 删除数据
    public static void deleteDta(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //指定列族删除数据
//        delete.addFamily(Bytes.toBytes(cf));
        //指定列族和列
//        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));
        table.delete(delete);
        table.close();
        stop();
    }

    public static void main(String[] args) throws IOException {
//        putData("stu2","1001","info","name","maow");
//        getData("stu2","1001","info","name");
//        scanTable("stu2");
        deleteDta("stu2","1001","info","name");
    }
}
