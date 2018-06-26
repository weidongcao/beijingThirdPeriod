package com.rainsoft.j2se;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 2017-06-14.
 */
public class Array2Map {
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        Map<String, String> cmap = new ConcurrentHashMap<>();
        Map<String, String> hmap = new HashMap<>();
        String md5 = DigestUtils.md5Hex("wedo");
        System.out.println(md5);
        for (int i = 0; i < 20; i++) {
            String key = RandomStringUtils.randomAlphanumeric(6);
            String value = i + "";
            hmap.put(key, value);
        }
        System.out.println("HashMap --> " + hmap.toString());
    }

    /**
     * @param plainText 明文
     * @return 32位密文
     */
    public String md5(String plainText) {
        String re_md5 = new String();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(plainText.getBytes());
            byte b[] = md.digest();

            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            re_md5 = buf.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return re_md5;
    }

}
