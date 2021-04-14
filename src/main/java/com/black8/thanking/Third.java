package com.black8.thanking;

import static com.black8.thanking.First.TAXI_FARE_FILENAME;
import static com.black8.thanking.First.TAXI_RADE_FILENAME;

import com.black8.thanking.Second.TaxiFare;
import com.black8.thanking.Second.TaxiRade;
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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangbaoming
 * @date 2020/3/18 9:48 下午
 */
@Slf4j
public class Third {

    public static void main(String[] args) throws IOException {
        List<TaxiRade> taxiRades = read(TAXI_RADE_FILENAME, new TaxiRadeRowMapper());
        List<TaxiFare> taxiFares = read(TAXI_FARE_FILENAME, new TaxiFareRowMapper());
        List<Driver> drivers = joinTaxiRadeFareByDriveId(taxiRades, taxiFares);
        drivers.forEach(driver -> log.info("司机[{}], 行程总耗时:{}分, 总收入：{}$",
            driver.getDriveId(), driver.getTotalTravelTime(), driver.getTotalFare()));
    }

    private static List<Driver> joinTaxiRadeFareByDriveId(List<TaxiRade> taxiRades, List<TaxiFare> taxiFares) {
        Map<String, Driver> driverMap = new HashMap<>();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        taxiRades.forEach(taxiRade -> {
            try {
                Date d1 = dateFormat.parse(taxiRade.getEndTime());
                Date d2 = dateFormat.parse(taxiRade.getStartTime());
                long diff = d1.getTime() - d2.getTime();
                long minutes = diff / (1000 * 60);
                Driver driver = new Driver();
                driver.setDriveId(taxiRade.getDriveId());
                driver.setTotalTravelTime(minutes);
                driverMap.put(taxiRade.getDriveId(), driver);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        });
        taxiFares.forEach(taxiFare -> {
            Driver driver = driverMap.get(taxiFare.getDriveId());
            if (driver != null) {
                driver.setTotalFare(taxiFare.getTotalFace());
            }
        });
        return new ArrayList<>(driverMap.values());
    }

    private static <T> List<T> read(String fileName, RowMapper<T> rowMapper) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        List<T> list = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            list.add(rowMapper.map(line));
        }
        return list;
    }

    private interface RowMapper<T> {

        T map(String row);
    }

    private static class TaxiRadeRowMapper implements RowMapper<TaxiRade> {

        @Override
        public TaxiRade map(String row) {
            String[] arr = row.split(",");
            TaxiRade taxiRade = new TaxiRade();
            taxiRade.setDriveId(arr[0]);
            taxiRade.setStartTime(arr[3]);
            taxiRade.setEndTime(arr[2]);
            return taxiRade;
        }
    }

    private static class TaxiFareRowMapper implements RowMapper<TaxiFare> {

        @Override
        public TaxiFare map(String row) {
            String[] arr = row.split(",");
            Second.TaxiFare taxiFare = new Second.TaxiFare();
            taxiFare.setDriveId(arr[0]);
            taxiFare.setTotalFace(arr[7]);
            return taxiFare;
        }
    }

    @Data
    private static class Driver {

        private String driveId;

        private long totalTravelTime;

        private String totalFare;
    }
}
