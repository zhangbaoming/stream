package com.black8.thanking;

import static com.black8.thanking.First.TAXI_FARE_FILENAME;
import static com.black8.thanking.First.TAXI_RADE_FILENAME;

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
 * @date 2020/3/4 5:44 下午
 */
@Slf4j
public class Second {

    public static void main(String[] args) throws IOException {
        List<TaxiRade> taxiRades = getTaxiRades();
        Map<String, Long> driveTravelTime = new HashMap<>();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        taxiRades.forEach(taxiRade -> {
            try {
                Date d1 = dateFormat.parse(taxiRade.getEndTime());
                Date d2 = dateFormat.parse(taxiRade.getStartTime());
                long diff = d1.getTime() - d2.getTime();
                long minutes = diff / (1000 * 60);
                driveTravelTime.put(taxiRade.getDriveId(), minutes);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        });

        List<TaxiFare> taxiFares = getTaxiFares();
        taxiFares.forEach(taxiFare -> {
            Long travelTime = driveTravelTime.get(taxiFare.getDriveId());
            if (travelTime != null) {
                log.info("司机[{}], 行程总耗时:{}分, 总收入：{}$",
                    taxiFare.getDriveId(), travelTime, taxiFare.getTotalFace());
            }
        });
    }

    private static List<TaxiRade> getTaxiRades() throws IOException {
        BufferedReader taxiRideDataReader = new BufferedReader(new FileReader(TAXI_RADE_FILENAME));
        String line;
        List<TaxiRade> taxiRades = new ArrayList<>();
        while ((line = taxiRideDataReader.readLine()) != null) {
            String[] arr = line.split(",");
            TaxiRade taxiRade = new TaxiRade();
            taxiRade.setDriveId(arr[0]);
            taxiRade.setStartTime(arr[3]);
            taxiRade.setEndTime(arr[2]);
            taxiRades.add(taxiRade);
        }
        return taxiRades;
    }

    private static List<TaxiFare> getTaxiFares() throws IOException {
        BufferedReader taxiFareDataReader = new BufferedReader(new FileReader(TAXI_FARE_FILENAME));
        String line;
        List<TaxiFare> taxiFares = new ArrayList<>();
        while ((line = taxiFareDataReader.readLine()) != null) {
            String[] arr = line.split(",");
            TaxiFare taxiFare = new TaxiFare();
            taxiFare.setDriveId(arr[0]);
            taxiFare.setTotalFace(arr[7]);
            taxiFares.add(taxiFare);
        }
        return taxiFares;
    }

    @Data
    static class TaxiRade {

        private String driveId;

        private String startTime;

        private String endTime;
    }

    @Data
    static class TaxiFare {

        private String driveId;

        private String totalFace;
    }
}
