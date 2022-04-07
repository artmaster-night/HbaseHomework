package com.master.test;

import com.master.util.HbaseFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;


/**
 *
 */
public class HbaseTableTest {
    private final HbaseFactory hbaseFactory=new HbaseFactory();

//    createTable(”score”,new String[]{”sname”,”course”});
    @Test
    public void createTable() throws IOException {
        TableDescriptorBuilder score = TableDescriptorBuilder.newBuilder(TableName.valueOf("score"));
        ColumnFamilyDescriptor sname = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("sname")).build();
        ColumnFamilyDescriptor course = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("course")).build();
        score.setColumnFamily(sname);
        score.setColumnFamily(course);
        hbaseFactory.getAdmin().createTable(score.build());
    }


    @Test
    public void searchTable() throws IOException {
        Table score = hbaseFactory.getConnection().getTable(TableName.valueOf("score"));

        ResultScanner scanner = score.getScanner(new Scan());
        for(Result result:scanner){
            Cell[] cells = result.rawCells();
            for (Cell cell:cells){
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("rowKey->" + rowKey + "\tfamily->" + family + "\tqualifier->" + qualifier + "\tvalue->" + value);
            }
        }
        score.close();
    }
    
    @Test
    public void deleteTable() throws IOException {
        TableName score = TableName.valueOf("score");
        hbaseFactory.getAdmin().disableTable(score);
        hbaseFactory.getAdmin().deleteTable(score);
    }

//    insertRow(”score”,”95001”,”sname”,””,”Mary”)
//    insertRow("Score", "95001", "course", "Math", "88");
//    insertRow("Score", "95001", "course", "English", "85");

    @Test
    public void insertData() throws IOException {
        Table score = hbaseFactory.getConnection().getTable(TableName.valueOf("score"));
        Put put = new Put(Bytes.toBytes("95001"));
        put.addColumn(Bytes.toBytes("sname"),Bytes.toBytes(""),Bytes.toBytes("Mary"));
        put.addColumn(Bytes.toBytes("course"),Bytes.toBytes("Math"),Bytes.toBytes("88"));
        put.addColumn(Bytes.toBytes("course"),Bytes.toBytes("English"),Bytes.toBytes("85"));
        score.put(put);
        score.close();
    }


//    查询Score表中，行键为95001，列族为course，列为Math的值
//    getData("Score", "95001", "course", "Math");
//    getData("Score", "95001", "course", "English");

    @Test
    public void searchData() throws IOException {
        Table score = hbaseFactory.getConnection().getTable(TableName.valueOf("score"));
        Get get1 = new Get(Bytes.toBytes("95001")).addColumn(Bytes.toBytes("course"), Bytes.toBytes("Math"));
        Get get2 = new Get(Bytes.toBytes("95001")).addColumn(Bytes.toBytes("course"),Bytes.toBytes("English"));
        Result result1 = score.get(get1);
        Result result2 = score.get(get2);
        Cell[] cells1 = result1.rawCells();
        Cell[] cells2 = result2.rawCells();
        for(Cell cell:cells1){
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("value = " + value);
        }
        for(Cell cell:cells2){
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("value = " + value);
        }
        score.close();
    }




//    删除指定行命令：insertRow(“score”,”95001”,””,””)
//
//    删除指定列族数据insertRow(“score”,”95001”,”sname”,””)
//    对应JAVA程序添加：delete.addFamily(colFamily.getBytes());
//
//    删除指定列数据insertRow(“score”,”95001”,”course”,”Math”)
//    对应JAVA程序添加：delete.addColumn(colFamily.getBytes(), col.getBytes());

    @Test
    public void deleteData() throws IOException {
        Table score = hbaseFactory.getConnection().getTable(TableName.valueOf("score"));
        Delete delete1 = new Delete(Bytes.toBytes("95001"));
        Delete delete2 = new Delete(Bytes.toBytes("95001"));
        delete2.addFamily(Bytes.toBytes("sname"));
        Delete delete3 = new Delete(Bytes.toBytes("95001"));
        delete3.addColumn(Bytes.toBytes("course"),Bytes.toBytes("Math"));
        score.delete(delete2);
        score.close();
    }


}
