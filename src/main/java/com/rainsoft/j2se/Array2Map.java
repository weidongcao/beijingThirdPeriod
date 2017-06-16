package com.rainsoft.j2se;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.SaslOutputStream;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by Administrator on 2017-06-14.
 */
public class Array2Map {
    public static void main(String[] args) throws IOException {
        File file = FileUtils.getFile("D:\\0WorkSpace\\JetBrains\\beijingThirdPeriod\\createIndexRecord\\index-record.txt");
        String str = FileUtils.readFileToString(file);
        JSONObject jsonObject = JSONObject.fromObject(str);
//        JSONArray jsonArray = JSONArray.fromObject(list);
//        System.out.println(jsonArray.join("\r\n"));
        System.out.println(jsonObject.toString(4));
        System.out.println(jsonObject.toJSONArray(new JSONArray()).toString());

    }
}
