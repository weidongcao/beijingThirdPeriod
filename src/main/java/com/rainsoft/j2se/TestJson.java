package com.rainsoft.j2se;

import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by CaoWeiDong on 2017-07-29.
 */
public class TestJson {
    public static void main(String[] args) throws IOException {
        InputStream jsonStream = TestJson.class.getClassLoader().getResourceAsStream("oracleTableField.json");
        String jsonString = IOUtils.toString(jsonStream);
        JSONObject jsonObject = JSONObject.fromObject(jsonString);
        System.out.println(jsonObject.optJSONArray("reg_content_ftp"));

    }
}
