package com.black8.thanking;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangbaoming
 * @date 2020/3/3 7:16 下午
 */
@Slf4j
public class First {
    static final String TAXI_FARE_FILENAME
        = "/Users/zhangbaoming/Project/java/stream/src/main/resources/nycTaxiFares.txt";

    static final String TAXI_RADE_FILENAME
        = "/Users/zhangbaoming/Project/java/stream/src/main/resources/nycTaxiRides.txt";

    public static void main(String[] args) throws IOException {
        // 1. 加注释
        // 2. 时间格式化提取
        // 3. 可读性很差，抽取方法
        // 4. 命名
        // 5. 文件读取抽取方法
        FileReader fileReader1 = new FileReader(TAXI_RADE_FILENAME);
        BufferedReader taxiRideDataReader = new BufferedReader(fileReader1);
        String line1;
        Map<String, Long> driveTravelTime = new HashMap<>();
        List<String> taxiRideDatas = new ArrayList<>();
        while ((line1 = taxiRideDataReader.readLine()) != null) {
            taxiRideDatas.add(line1);
        }

        taxiRideDatas.forEach(taxiRideData -> {
            String[] arr = taxiRideData.split(",");
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                Date d1 = dateFormat.parse(arr[2]);
                Date d2 = dateFormat.parse(arr[3]);
                long diff = d1.getTime() - d2.getTime();
                long minutes = diff / (1000 * 60);
                driveTravelTime.put(arr[0], minutes);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        });

        FileReader fileReader2 = new FileReader(TAXI_FARE_FILENAME);
        BufferedReader taxiFareDataReader = new BufferedReader(fileReader2);
        String line2;
        List<String> taxiFareDatas = new ArrayList<>();
        while ((line2 = taxiFareDataReader.readLine()) != null) {
            taxiFareDatas.add(line2);
        }

        List<String> results = new ArrayList<>();
        taxiFareDatas.forEach(taxiFareData -> {
            String[] arr = taxiFareData.split(",");
            Long spendTime = driveTravelTime.get(arr[0]);
            results.add(arr[0] + "," + spendTime + "," + arr[7]);
        });

        for (String result : results) {
            String[] arr = result.split(",");
            if (arr.length == 3) {
                log.info("司机[{}], 行程总耗时:{}分, 总收入：{}$", arr[0], arr[1], arr[2]);
            }
        }
    }
}
