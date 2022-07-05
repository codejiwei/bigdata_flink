package com.codejiwei.java_demo;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: jiwei01
 * Date: 2022/7/3 14:30
 * Package: com.codejiwei.java_demo
 * Description:
 */
public class ForLoopTest {
    public static void main(String[] args) {
        Map<Integer, String> map1 = new HashMap<Integer, String>();
        map1.put(1, "aaa");
        map1.put(2, "bbb");
        map1.put(3, "ccc");

        for (Map.Entry<Integer, String> entry: map1.entrySet()) {
            System.out.println("key:" + entry.getKey() + ", value:" + entry.getValue());
        }

//        map1.forEach((k, v) -> System.out.println("key:" + k + "value:" + v));



    }
}