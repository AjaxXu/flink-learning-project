package com.louis.flink.streaming.wordcount;

import com.louis.flink.streaming.wordcount.util.WordCountData;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Louis
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.fromElements(WordCountData.WORDS);
        source.flatMap(new FlatMapFunction<String, WordAndCount>() {
            @Override
            public void flatMap(String s, Collector<WordAndCount> collector) throws Exception {
                String[] words = s.split(" ");
                for (String w: words) {
                    collector.collect(new WordAndCount(w, 1L));
                }
            }
        }).filter(new FilterFunction<WordAndCount>() {
            @Override
            public boolean filter(WordAndCount wordAndCount) throws Exception {
                if (wordAndCount.getWord().length() > 2) {
                    return true;
                } else {
                    return false;
                }
            }
        }).keyBy("word").sum("count").filter(new FilterFunction<WordAndCount>() {
            @Override
            public boolean filter(WordAndCount wordAndCount) throws Exception {
                if (wordAndCount.getCount() > 5) return true;
                return false;
            }
        }).print();
        env.execute("word count");
    }

    public static class WordAndCount {
        private String word;
        private Long count;

        public WordAndCount() {
        }

        public WordAndCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordAndCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
