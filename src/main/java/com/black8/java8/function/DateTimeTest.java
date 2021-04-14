package com.black8.java8.function;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author zhangbaoming
 * @date 2020/12/3 8:23 下午
 */
public class DateTimeTest {

    public static void main(String[] args) {
        LocalDateTime now = LocalDateTime.parse("2020-10-15T10:20:34.123Z",
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
        System.out.println("now. = " + now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }
}
