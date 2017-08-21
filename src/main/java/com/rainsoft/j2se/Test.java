package com.rainsoft.j2se;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by Administrator on 2017-06-22.
 */
public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) throws IOException {
        File file = FileUtils.getFile("E:\\data\\test\\test.txt");
        String str = FileUtils.readFileToString(file, "utf-8");
        System.out.println(str);
        str = str.replace("\t", "").replace("\r\n", "").replace("\r", "").replace("\n", "");
        System.out.println(str);
    }
}
