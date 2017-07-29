package com.rainsoft.spring;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.util.StringUtils;

/**
 * Created by CaoWeiDong on 2017-07-19.
 */
public class TestPropertyPlaceholderHelper {
    public static final String PREFIX = "${";
    public static final String SUFFIX = "}";

    private static String parseStringValue(String strVal, Set<String> set) {
        StringBuilder buf = new StringBuilder(strVal);
        int startIndex = buf.indexOf(PREFIX);

        //递归的出口：没有变量需要替换了
        if (startIndex < 0) {
            return buf.toString();
        }

        while (true) {
            int endIndex = findPlaceholderEndIndex(buf, startIndex);
            if (endIndex == -1) {
                break;
            }

            //找到了最外层的变量，例如"${x${abc}x}"里面的"x${abc}x"--显然，变量里还有变量；这就是为什么要用递归
            String placeHolder = buf.substring(startIndex + PREFIX.length(), endIndex);

            //确保后面移出的变量是我们放进去的变量（而不是解析过程中产生的、新的变量，见求解"${top}"的例子）
            String originPlaceHolder = placeHolder;

            //防止循环定义：例如strVal="${a}"，而valueMap=[a="${b}", b="${a}"]
            if (!set.add(originPlaceHolder)) {
                throw new IllegalArgumentException("circular placeholder");
            }

            //递归对"变量里的变量"求解，直到最里面的变量求解得出结果为止，再一层一层的向外层返回
            placeHolder = parseStringValue(placeHolder, set);

            //可能value里面还有变量，需要递归求解
            String val = parseStringValue(valueMap.get(placeHolder), set);

            if (val != null) {
                buf.replace(startIndex, endIndex + SUFFIX.length(), val);

                //继续向后替换变量："${a}xx${b}"，替换好${a}之后，继续替换${b}
                startIndex = buf.indexOf(PREFIX, startIndex + val.length());
            }
            set.remove(originPlaceHolder);  //注意这里
            //set.remove(placeHolder);  //bug!
        }

        return buf.toString();

    }

    /**
     * 查找与PREFIX配对的SUFFIX
     * 注意处理嵌套的情况：用within变量来记录
     * 例如${ab${cd}}，startIndex指向a，从a开始找，当找到"${"时，within为1；
     * 找到第一个"}"时，within减1，抵消前面的"${"；while循环继续，直到找到最后的"}"
     * 这有点像"利用栈来判断括号是否配对"
     */
    private static int findPlaceholderEndIndex(StringBuilder buf, int startIndex) {
        int within = 0;
        int index = startIndex + PREFIX.length();
        while (index < buf.length()) {

            //发现了一个嵌套的PREFIX
            if (StringUtils.substringMatch(buf, index, PREFIX)) {
                within++;
                index = index + PREFIX.length();

                //发现了一个嵌套的SUFFIX，因此抵消一个PREFIX：within减1
            } else if (StringUtils.substringMatch(buf, index, SUFFIX)) {
                if (within > 0){
                    within--;
                    index = index + SUFFIX.length();
                } else if (within == 0){
                    return index;
                }
            } else {
                index++;
            }
        }
        return -1;
    }

    //for test
    private static Map<String, String> valueMap = new HashMap<String, String>();
    static {
        valueMap.put("inner", "ar");
        valueMap.put("bar", "ljn");

        //求解"${top}"的过程中，第一次递归返回时，${differentiator}被替换为"first"，然后产生
        //了一个新的placeHolder="first.grandchild"，这个placeHolder用"actualValue"替换
        //然后移除变量，如果set.remove(placeHolder)的话，那移除的就是"first.grandchild"
        //而实际上，它应该移除的是"${differentiator}.grandchild"。这个bug难以发现
        valueMap.put("top", "${child}+${child}");
        valueMap.put("child", "${${differentiator}.grandchild}");
        valueMap.put("differentiator", "first");
        valueMap.put("first.grandchild", "actualValue");
    }

    public static void main(String[] args) {
        String s ="${top}";
        System.out.println(parseStringValue(s, new HashSet<String>())); //actualValue+actualValue

        s = "foo=${b${inner}}-${bar}-end";
        System.out.println(parseStringValue(s, new HashSet<String>())); //foo=ljn-ljn-end

    }
}
