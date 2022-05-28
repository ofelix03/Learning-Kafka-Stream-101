package transformations.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * A streams processing application that takes a source stream, and the Apache Kafka keywords and performs an inverse
 * filtering. Any message that are not contained in the whitelisted keywords
 * are published to the topic "inverse-inverse-filter-stream-output".
 *
 * Whitelisted keywords: kafka, fault-tolerant, scalable, distributable, windowing, stream, processor
 */
public class InverseFilterStream {

    public static String WHITELIST_REGEX = "(\\w|\\s)*(kafka|fault-tolerant|scalable|distributable|windowing|stream|processor)(\\w|\\s)*";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "inverse-filter-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder  builder = new StreamsBuilder();
        KStream<String, String>  source = builder.stream("inverse-filter-stream-input");
        source.filterNot((key, value) -> value.toLowerCase().matches(WHITELIST_REGEX)).to("inverse-filter-stream-output");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
