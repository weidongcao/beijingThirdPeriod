package com.rainsoft.j2se;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2017-06-14.
 */
public class Array2Map {
    public static void main(String[] args) throws IOException {
        File file = FileUtils.getFile("D:\\0WorkSpace\\JetBrains\\beijingThirdPeriod\\createIndexRecord\\index-record.txt");
        String str = FileUtils.readFileToString(file, "utf-8");
        JSONObject jsonObject = JSONObject.fromObject(str);
//        JSONArray jsonArray = JSONArray.fromObject(list);
//        System.out.println(jsonArray.join("\r\n"));
        System.out.println(jsonObject.toString(4));
        System.out.println(jsonObject.toJSONArray(new JSONArray()).toString());

    }
}
