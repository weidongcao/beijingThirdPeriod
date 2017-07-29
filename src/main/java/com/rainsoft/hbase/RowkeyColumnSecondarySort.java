package com.rainsoft.hbase;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 对HBase表的rowkey和column进行二次排序
 * 按rowkey排序，如果rowkey相同按column排序
 * Created by CaoWeidong on 2017-05-17.
 */
public class RowkeyColumnSecondarySort implements Ordered<RowkeyColumnSecondarySort>, Serializable {


    private static final long serialVersionUID = -1677704670472490266L;

    private String rowkey;
    private String column;

    public RowkeyColumnSecondarySort(String rowkey, String column) {
        this.rowkey = rowkey;
        this.column = column;
    }

    public RowkeyColumnSecondarySort() {

    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    @Override
    public int compare(RowkeyColumnSecondarySort that) {
        if (rowkey.compareTo(that.getRowkey()) == 0) {
            return column.compareTo(that.getColumn());
        } else {
            return rowkey.compareTo(that.getRowkey());
        }
    }

    @Override
    public boolean $less(RowkeyColumnSecondarySort that) {
        if (rowkey.compareTo(that.getRowkey()) < 0) {
            return true;
        } else if ((rowkey.compareTo(that.getRowkey()) == 0)
                && (column.compareTo(that.getColumn()) < 0)) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater(RowkeyColumnSecondarySort that) {
        if ((rowkey.compareTo(that.getRowkey()) > 0)) {
            return true;
        } else if ((rowkey.compareTo(that.getRowkey()) == 0)
                && (column.compareTo(that.getColumn()) > 0)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(RowkeyColumnSecondarySort that) {
        if ($less(that)) {
            return true;
        } else if ((rowkey.compareTo(that.getRowkey()) == 0)
                && (column.compareTo(that.getColumn()) == 0)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(RowkeyColumnSecondarySort that) {
        if ($greater(that)) {
            return true;
        } else if ((rowkey.compareTo(that.getRowkey()) == 0)
                && (column.compareTo(that.getColumn()) == 0)) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(RowkeyColumnSecondarySort that) {
        return compare(that);
    }
}
