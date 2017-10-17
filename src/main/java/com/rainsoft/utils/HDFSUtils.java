package com.rainsoft.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Created by CaoWeiDong on 2017-08-06.
 */
public class HDFSUtils {
    public static final Configuration conf = new Configuration();
    public static FileSystem fs = null;

    static {
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
