package com.rainsoft.j2se;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by CaoWeiDong on 2017-12-25.
 */
public class TenMinWalk {
    public static Map<Character, Integer> map = new HashMap<>();
    static {
        map.put('n', 1);
        map.put('s', -1);
        map.put('e', 100);
        map.put('w', -100);
    }
    public static boolean isValid(char[] walk) {

        int flat = 0;
        if (walk.length != 10) {
            return false;
        } else {
            for (int i = 0; i < 10; i++) {
                int offset = map.get(walk[i]);
                flat += offset;
            }
        }
        if (flat == 0) {
            return true;
        } else {
            return false;
        }
    }

    public static void main(String[] args) {
        assertTrue("Should return true", TenMinWalk.isValid(new char[]{'n', 's', 'n', 's', 'n', 's', 'n', 's', 'n', 's'}));
        assertFalse("Should return false", TenMinWalk.isValid(new char[]{'w', 'e', 'w', 'e', 'w', 'e', 'w', 'e', 'w', 'e', 'w', 'e'}));
        assertFalse("Should return false", TenMinWalk.isValid(new char[]{'w'}));
        assertFalse("Should return false", TenMinWalk.isValid(new char[]{'n', 'n', 'n', 's', 'n', 's', 'n', 's', 'n', 's'}));
    }


}
