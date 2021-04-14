package com.black8.streaming;

import lombok.Data;

/**
 *
 * @author zhangbaoming
 * @date 2020/4/27 2:45 下午
 */
@Data
public class WordFrequency {

    private String word;
    private long frequency;

    public WordFrequency() {
    }

    WordFrequency(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WC " + word + " " + frequency;
    }
}
