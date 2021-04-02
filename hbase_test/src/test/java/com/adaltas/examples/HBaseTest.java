package com.adaltas.examples;
import java.io.IOException;

import static org.junit.Assert.*;


    import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;


public class HBaseTest {
private static HBaseTestingUtility utility;
byte[] CF = "CF".getBytes();
byte[] CQ1 = "CQ-1".getBytes();
@Before
public void setup() throws Exception {
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
}

@Test
    public void testInsert() throws Exception {
         HTableInterface table = utility.createTable(Bytes.toBytes("MyTest"), Bytes.toBytes("CF"));
     Put put = new Put(Bytes.toBytes("ROWKEY-1"));
     put.add(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1"), Bytes.toBytes("DATA-1"));
     put.add(Bytes.toBytes("CF"), Bytes.toBytes("CQ-2"), Bytes.toBytes("DATA-2"));
     table.put(put);
     Get get = new Get(Bytes.toBytes("ROWKEY-1"));
         get.addColumn(CF, CQ1);
         Result result = table.get(get);
         assertEquals(Bytes.toString(result.getRow()), "ROWKEY-1");
         assertEquals(Bytes.toString(result.value()), "DATA-1");

    }}

