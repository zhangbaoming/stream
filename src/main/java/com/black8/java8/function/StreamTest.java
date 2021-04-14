package com.black8.java8.function;


import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 * @author zhangbaoming
 * @date 2020/12/3 8:18 下午
 */
public class StreamTest {

    public static void main(String[] args) {
        ArrayList<Integer> lists = Lists.newArrayList(1, 2, 13, 4, 15, 6, 17, 8, 19);
        long count = lists.stream().distinct().count();
        System.out.println("count = " + count);
    }
}
