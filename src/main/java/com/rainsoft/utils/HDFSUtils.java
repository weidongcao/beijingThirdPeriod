package com.rainsoft.utils;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

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
