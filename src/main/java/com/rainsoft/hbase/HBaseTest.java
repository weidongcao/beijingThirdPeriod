package com.rainsoft.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Administrator on 2017-04-19.
 */
public class HBaseTest {

    private static Connection conn;

    static {
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conf.addResource("hbase-site.xml");
            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    public static void main(String[] args) throws IOException {
    }

    /* test create table. */
    public static void createTable(String tableName, String[] family)
            throws Exception {
        Admin admin = conn.getAdmin();

        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor desc = new HTableDescriptor(tn);
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }

        if (admin.tableExists(tn)) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }

    /* put data into table. */
    public static void addData(String rowKey, String tableName,
                               String[] column1, String[] value1, String[] column2, String[] value2)
            throws IOException {
        /* get table. */
        TableName tn = TableName.valueOf(tableName);
        Table table = conn.getTable(tn);

        /* create put. */
        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String f = columnFamilies[i].getNameAsString();
            if (f.equals("article")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (f.equals("author")) {
                for (int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }

        /* put data. */
        table.put(put);
        System.out.println("add data Success!");
    }

    /* get data. */
    public static void getResult(String tableName, String rowKey)
            throws IOException {
        /* get table. */
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            System.out.println("------------------------------------");
            System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
            System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
            System.out.println("timest: " + cell.getTimestamp());
        }
    }

    /* scan table. */
    public static void getResultScan(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    System.out.println("------------------------------------");
                    System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
                    System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
                    System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
                    System.out.println("timest: " + cell.getTimestamp());
                }
            }
        } finally {
            rs.close();
        }
    }

    /* range scan table. */
    public static void getResultScan(String tableName, String start_rowkey,
                                     String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    System.out.println("------------------------------------");
                    System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
                    System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
                    System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
                    System.out.println("timest: " + cell.getTimestamp());
                }
            }
        } finally {
            rs.close();
        }
    }
}
