package com.rainsoft.spark;

import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by CaoWeiDong on 2017-12-14.
 */
public class CreateAccompanyAnalysisData {
    public static Random random = new Random();
    public static void main(String[] args) throws IOException {
        createTrackGaussianData("E:\\data\\community-100.txt", "E:\\data\\name-5w.txt", "E:\\data\\track-1w.txt", 10, 1000);

    }

    public static void createTrackGaussianData(String communityPath, String namePath, String outputPath, int communityNum, int nameNum) throws IOException {
        List<String> communities = FileUtils.readLines(FileUtils.getFile(communityPath), "utf-8");
        List<String> names = FileUtils.readLines(FileUtils.getFile(namePath), "utf-8");

        int index;
        String column1, column2;
        List<String> data = new ArrayList<>();
        for (int i = 0; i < nameNum; i++) {
            index = random.nextInt(names.size());
            column2 = names.get(index);
            for (int j = 0; j < communityNum; j++) {
                index = getGaussianNum(10000, 50, 100, -1);
                column1 = communities.get(index);
                data.add(String.join(",", column1, column2));
            }
        }
        FileUtils.writeStringToFile(FileUtils.getFile(outputPath), StringUtils.join(data, "\r\n"), "utf-8", false);
    }
    public static void createTrackRandomData(String communityPath, String namePath, String outputPath, int communityNum, int peopleNum) throws IOException {
        List<String> communities = FileUtils.readLines(FileUtils.getFile(communityPath), "utf-8");
        List<String> names = FileUtils.readLines(FileUtils.getFile(namePath), "utf-8");
        List<String> coms = communities.subList(0, 100);

        int index;
        String name;
        String community;
        List<String> list = new ArrayList<>();
        for (int i = 0; i < peopleNum; i++) {
            index = random.nextInt(names.size());
            name = names.get(index);
            for (int j = 0; j < communityNum; j++) {
                index = random.nextInt(communities.size());
                community = communities.get(index);
                list.add(String.join(",", community, name));
            }
        }
        FileUtils.writeStringToFile(FileUtils.getFile(outputPath), StringUtils.join(list, "\r\n"), "utf-8", false);
    }

    public static int getGaussianNum(int squire, int avg, int maxNum, int minNum) {
        int num;
        while (true) {
            num = (int) (Math.sqrt(squire) * random.nextGaussian() + avg);
            if (num < maxNum && num > minNum) {
                break;
            }
        }
        return num;
    }

}
