package org.helloworld;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class EsTest {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT,"8081");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<Article> source = environment.addSource(source());
        List<HttpHost> esHttphost = new ArrayList<>();
        esHttphost.add(new HttpHost("127.0.0.1", 9200, "http"));
        ElasticsearchSink.Builder<Article> esBuilder = new ElasticsearchSink.Builder<>(esHttphost, (ElasticsearchSinkFunction<Article>) (article, runtimeContext, requestIndexer) -> {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(article.getId()+"");
            indexRequest.source(article.toMap());
            indexRequest.index("article_from_filnk");
            requestIndexer.add(indexRequest);
        });
        esBuilder.setBulkFlushMaxActions(10);
        esBuilder.setBulkFlushInterval(10000);
        esBuilder.setBulkFlushMaxSizeMb(100);
        source.addSink(esBuilder.build());
        environment.execute();
    }

    private static SourceFunction<Article> source(){
        return new SourceFunction<>() {
            volatile boolean flag = false;
            final AtomicLong idGenerator = new AtomicLong();

            @Override
            public void run(SourceContext<Article> ctx) throws InterruptedException {
                while (!flag) {
                    Article article = new Article();
                    long id = idGenerator.getAndIncrement();
                    article.setId(id);
                    article.setName("name" + id);
                    article.setContent("content" + id);
                    Date publishTime = new Date();
                    article.setPublishTime(publishTime);

                    ctx.collectWithTimestamp(article, publishTime.getTime());
                    Watermark watermark = new Watermark(publishTime.getTime()-1);
                    ctx.emitWatermark(watermark);
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {
                flag = true;
            }
        };
    }
}
