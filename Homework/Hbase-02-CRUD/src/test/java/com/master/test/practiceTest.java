package com.master.test;

import com.master.util.HbaseFactory;
import javafx.scene.control.Tab;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.Computer;

import java.io.IOException;

public class practiceTest {
    private final HbaseFactory hbaseFactory=new HbaseFactory();

    //    课程表（Course）
    //    课程号（C_No）	课程名（C_Name）	学分（C_Credit）
    @Test
    public void t1() throws IOException {
        TableDescriptorBuilder course = TableDescriptorBuilder.newBuilder(TableName.valueOf("Course"));
        ColumnFamilyDescriptor c_name = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C_Name")).build();
        ColumnFamilyDescriptor c_credit = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C_Credit")).build();
        course.setColumnFamily(c_credit);
        course.setColumnFamily(c_name);
        hbaseFactory.getAdmin().createTable(course.build());
    }

//    课程表（Course）
//    课程号（C_No）	课程名（C_Name）	学分（C_Credit）
//            123001	Math	2.0
//            123002	Computer Science	5.0
//            123003	English	3.0

    @Test
    public void t2() throws IOException {
        Table course = hbaseFactory.getConnection().getTable(TableName.valueOf("Course"));
        
        Put put1 = new Put(Bytes.toBytes("123001"));
        put1.addColumn(Bytes.toBytes("C_Name"), Bytes.toBytes(""), Bytes.toBytes("Math"));
        put1.addColumn(Bytes.toBytes("C_Credit"),Bytes.toBytes(""),Bytes.toBytes("2.0"));
        Put put2 = new Put(Bytes.toBytes("123002"));
        put2.addColumn(Bytes.toBytes("C_Name"), Bytes.toBytes(""), Bytes.toBytes("Computer Science"));
        put2.addColumn(Bytes.toBytes("C_Credit"),Bytes.toBytes(""),Bytes.toBytes("5.0"));
        Put put3 = new Put(Bytes.toBytes("123003"));
        put3.addColumn(Bytes.toBytes("C_Name"), Bytes.toBytes(""), Bytes.toBytes("English"));
        put3.addColumn(Bytes.toBytes("C_Credit"),Bytes.toBytes(""),Bytes.toBytes("3.0"));
        course.put(put1);
        course.put(put2);
        course.put(put3);
        
        course.close();
    }

//    查询数据：查询Course表中，行键为123002，列族为C_Name的值，
    @Test
    public void t3() throws IOException {
        Table course = hbaseFactory.getConnection().getTable(TableName.valueOf("Course"));

        Get get = new Get(Bytes.toBytes("123002"));
        get.addColumn(Bytes.toBytes("C_Name"),Bytes.toBytes(""));
        Result result = course.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell:cells){
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("row->" + row + "\tfamily->" + family + "\tvalue->" + value);
        }

        course.close();
    }

//    123004	Chemistry	3.0
    @Test
    public void t4() throws IOException {
        Table course = hbaseFactory.getConnection().getTable(TableName.valueOf("Course"));

        Put put = new Put(Bytes.toBytes("123004"));
        put.addColumn(Bytes.toBytes("C_Name"),Bytes.toBytes(""),Bytes.toBytes("Chemistry"));
        put.addColumn(Bytes.toBytes("C_Credit"),Bytes.toBytes(""),Bytes.toBytes("3.0"));
        course.put(put);
        
        course.close();
    }

//    （5）删除数据：删除Course表中，行键为123001的值，	给出运行结果与终端shell查询截图；
    @Test
    public void t5() throws IOException {
        Table course = hbaseFactory.getConnection().getTable(TableName.valueOf("Course"));

        Delete delete = new Delete(Bytes.toBytes("123001"));
        course.delete(delete);

        course.close();
    }
}

