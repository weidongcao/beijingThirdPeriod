package com.rainsoft.guava;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.junit.Test;

import java.util.*;

/**
 * 代码用于记录字符串在数组中出现的次数。这种场景在实际的开发过程中不是容易经常出现的，
 * 如果使用实现Multiset接口的具体类就可以很容易实现以上的功能需求。
 * Created by CaoWeiDong on 2018-02-01.
 */
public class MuliCollectionTest {
    public void testWordCount() {
        String strWorld = "wer|dffd|ddsa|dfd|dreg|de|dr|ce|ghrt|cf|gt|ser|tg|ghrt|cf|gt|" +
                "ser|tg|gt|kldf|dfg|vcd|fg|gt|ls|lser|dfr|wer|dffd|ddsa|dfd|dreg|de|dr|" +
                "ce|ghrt|cf|gt|ser|tg|gt|kldf|dfg|vcd|fg|gt|ls|lser|dfr";

        String[] words = strWorld.split("\\|");

        Map<String, Integer> cntMap = new HashMap<>();
        for (String word : words) {
            cntMap.merge(word, 1, (a, b) -> a + b);
        }
        System.out.println("cntMap:");
        for (String key : cntMap.keySet()) {
            System.out.println(key + " count: " + cntMap.get(key));
        }
    }

    @Test
    public void testMultisetWordCount() {
        String strWorld = "wer|dffd|ddsa|dfd|dreg|de|dr|ce|ghrt|cf|gt|ser|tg|ghrt|cf|gt|" +
                "ser|tg|gt|kldf|dfg|vcd|fg|gt|ls|lser|dfr|wer|dffd|ddsa|dfd|dreg|de|dr|" +
                "ce|ghrt|cf|gt|ser|tg|gt|kldf|dfg|vcd|fg|gt|ls|lser|dfr";
        String[] words = strWorld.split("\\|");

        List<String> wordList = new ArrayList<>();
        Collections.addAll(wordList, words);

        Multiset<String> wordsMultiset = HashMultiset.create();
        wordsMultiset.addAll(wordList);

        for (String key : wordsMultiset.elementSet()) {
            System.out.println(key + " count: " + wordsMultiset.count(key));
        }
        if (!wordsMultiset.contains("dong")) {
            wordsMultiset.add("dong", 2);
        }
        System.out.println("--------------------------------------------------|");
    }

}
