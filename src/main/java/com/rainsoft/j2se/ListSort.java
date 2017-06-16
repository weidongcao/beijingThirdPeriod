package com.rainsoft.j2se;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Administrator on 2017-06-14.
 */
public class ListSort {
    public static void main(String[] args) throws IOException {
        File file = FileUtils.getFile("D:\\0WorkSpace\\JetBrains\\beijingThirdPeriod\\createIndexRecord\\index-record.txt");
        List<String> list = FileUtils.readLines(file);
        Collections.sort(list);

        Map<String, String> map = list.stream().collect(Collectors.toMap(str -> str.split("\t")[0], str -> str.split("\t")[1]));

//        String content = Stream.of(map)
//        List<String> listFromMap = map.entrySet().stream().collect(Collectors.toList(Map.Entry :: getKey + "\t", Map.Entry :: getValue));
//        map.entrySet().stream().flatMap(m -> {return  m.getKey() + "\t" + m.getValue();});
        JSONArray jsonArray = JSONArray.fromObject(list);
        System.out.println(jsonArray.join("\r\n"));

        JSONObject jsonObject = JSONObject.fromObject(map);
        System.out.println(jsonObject.toString());
    }

}
