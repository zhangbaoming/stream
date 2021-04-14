package com.black8.java8.function;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

/**
 *
 * @author zhangbaoming
 * @date 2020/12/3 7:25 下午
 */
public class FunctionTest {

    public static void main(String[] args) {
        print(Integer::parseInt, "20");
    }

    private static void print(Function<String, Integer> rowMapper, String s) {
        System.out.println(rowMapper.apply(s));
    }
}
