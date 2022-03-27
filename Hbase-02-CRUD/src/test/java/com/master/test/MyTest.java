package com.master.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class MyTest{
    @Test
    public void createTable()throws Exception{
//        获取配置文件
        Configuration config = HBaseConfiguration.create();
//        获取连接
        Connection connection = ConnectionFactory.createConnection(config);
//        拿到admin
        Admin admin = connection.getAdmin();

//        建表
        TableName dept = TableName.valueOf("dept");
        TableDescriptorBuilder deptBuilder = TableDescriptorBuilder.newBuilder(dept);
        TableName emp = TableName.valueOf("emp");
        TableDescriptorBuilder empBuilder = TableDescriptorBuilder.newBuilder(emp);
//        创建列族
        ColumnFamilyDescriptor descriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data")).build();
        deptBuilder.setColumnFamily(descriptor);
        empBuilder.setColumnFamily(descriptor);
        admin.createTable(deptBuilder.build());
        admin.createTable(empBuilder.build());

    }

    @Test
    public void insertData() throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        TableName tb_step2 = TableName.valueOf("tb_step2");
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tb_step2);
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data")).build();
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        admin.createTable(tableDescriptorBuilder.build());

//        插入数据
        Table table = connection.getTable(tb_step2);
        try {
            byte[] row1 = Bytes.toBytes("row1");
            byte[] qualifier1 = Bytes.toBytes(String.valueOf(1));
            byte[] value1 = Bytes.toBytes("张三丰");
            byte[] column = Bytes.toBytes("data");
            Put put1 = new Put(row1);
            put1.addColumn(column, qualifier1, value1);
            table.put(put1);


            byte[] row2 = Bytes.toBytes("row2");
            Put put2 = new Put(row2);

            byte[] qualifier2 = Bytes.toBytes(String.valueOf(2));

            byte[] value2 = Bytes.toBytes("张无忌");

            put2.addColumn(column, qualifier2, value2);

            table.put(put2);
        } finally {
            table.close();
        }
    }


    @Test
    public void getData() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);

        TableName t_step3 = TableName.valueOf("t_step3");
        Table table = connection.getTable(t_step3);
        Get row1 = new Get(Bytes.toBytes("row1"));
        Result result = table.get(row1);
        byte[] data = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("1"));
        String s = new String(data, "utf-8");
        System.out.println("value:" + s);


        Scan scan = new Scan();
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result scanResult : scanner) {
                byte[] row = scanResult.getRow();
                System.out.println("rowName:" + new String(row, "utf-8"));
            }
        }
    }
}