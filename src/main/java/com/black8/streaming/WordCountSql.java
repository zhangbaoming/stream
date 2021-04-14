package com.black8.streaming;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 *
 * @author zhangbaoming
 * @date 2020/4/27 2:38 下午
 */
public class WordCountSql {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        List<WordFrequency> list = new ArrayList<>();
        String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        for (String word : words) {
            WordFrequency wordFrequency = new WordFrequency(word, 1);
            list.add(wordFrequency);
        }
        DataSet<WordFrequency> input = env.fromCollection(list);
        tEnv.registerDataSet("WordCount", input, "word, frequency");
        Table table = tEnv.sqlQuery(
            "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
        DataSet<WordFrequency> result = tEnv.toDataSet(table, WordFrequency.class);
        result.print();
    }
}