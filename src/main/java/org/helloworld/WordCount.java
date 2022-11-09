package org.helloworld;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static String filePath = "C:\\Users\\wanp1\\IdeaProjects\\filnk\\flink_demo\\src\\main\\resources\\text\\words.txt";
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT,"8081");
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile(filePath);
        /*JoinedStreams<String, Object> join = stringDataStreamSource.join(null);
        DataStream<String> union = stringDataStreamSource.union(null);
        stringDataStreamSource.connect(union);*/
        SingleOutputStreamOperator<Word> sum = stringDataStreamSource.flatMap(getFlatMapper()).keyBy((KeySelector<Word, String>) Word::getWord).sum("count");
        sum.print();

        executionEnvironment.execute("wordcount");
    }

    private static FlatMapFunction<String, Word> getFlatMapper() {
        return new FlatMapFunction<String, Word>() {
            @Override
            public void flatMap(String s, Collector<Word> collector) throws Exception {
                Thread.sleep(5000);
                String[] split = s.split(",");
                for (String word : split) {
                    collector.collect(new Word(word, 1));
                }
            }
        };
    }



    private static void dataSetApi() throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> stringDataSource = executionEnvironment.readTextFile(filePath);

        AggregateOperator<Tuple2<String, Integer>> sum = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(",");
                for (String word : split) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).groupBy(0).sum(1);
        sum.print();
        executionEnvironment.execute();
    }
}

