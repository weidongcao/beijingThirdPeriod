package com.rainsoft.hbase;

import com.rainsoft.utils.HBaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.util.ArrayList;
import java.util.List;

public class HBaseApp {
    public static void main(String[] args) throws Exception {
        HBaseApp app = new HBaseApp();

        app.scanData("emp");

    }

    public void putData() {
        String tableName = "H_REG_CONTENT_FTP";
        List<Put> puts = new ArrayList<>();

        try {
            Table table = HBaseUtils.getTable(tableName);
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void scanData(String tableName) throws Exception {
        // Get table instance
        Table table = null;
        ResultScanner resultScanner = null;

        try {
            table = HBaseUtils.getTable(tableName);

            Scan scan = new Scan();
            resultScanner = table.getScanner(scan);

            // iterator
            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println(//
                            Bytes.toString(CellUtil.cloneFamily(cell))
                                    + ":" //
                                    + Bytes.toString(CellUtil.cloneQualifier(cell)) //
                                    + " -> " //
                                    + Bytes.toString(CellUtil.cloneValue(cell)) //
                                    + " " //
                                    + cell.getTimestamp()
                    );
                }
                System.out.println("--------------------------------");
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(resultScanner);
            IOUtils.closeStream(table);
        }

    }



}
