package com.master.test;

import com.master.util.HbaseFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class practiceTest {
    private final HbaseFactory hbaseFactory=new HbaseFactory();

    //    课程表（Course）
    //    课程号（C_No）	课程名（C_Name）	学分（C_Credit）
    @Test
    public void t1() throws IOException {
        TableDescriptorBuilder course = TableDescriptorBuilder.newBuilder(TableName.valueOf("Course"));
        ColumnFamilyDescriptor c_no = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C_No")).build();
        ColumnFamilyDescriptor c_name = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C_Name")).build();
        ColumnFamilyDescriptor c_credit = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C_Credit")).build();
        course.setColumnFamily(c_credit);
        course.setColumnFamily(c_no);
        course.setColumnFamily(c_name);
        hbaseFactory.getAdmin().createTable(course.build());
    }


}
