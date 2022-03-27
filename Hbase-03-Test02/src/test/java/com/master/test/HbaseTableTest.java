package com.master.test;

import com.master.util.HbaseFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

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
    }
    
    @Test
    public void deleteTable() {
        
    }
}
