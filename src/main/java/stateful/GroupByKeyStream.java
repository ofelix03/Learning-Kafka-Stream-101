package stateful;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * A streams processing application that takes a source stream, group and aggregate streams.
 * */
public class GroupByKeyStream {

    public static String INPUT_TOPIC = "groupby-key-stream-input";
    public static String OUTPUT_TOPIC = "groupby-key-stream-output";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "groupby-key-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder  builder = new StreamsBuilder();
        KStream<String, String>  source = builder.stream(INPUT_TOPIC);
        source.flatMapValues((key, value) -> Arrays.asList(value.split("\\s+")));
        source.peek((key, value) -> System.out.println(String.format("Peek:: %s - %s", key, value)));

        KTable<String, Long> table = source.flatMapValues((key, value) -> Arrays.asList(value.split("\\s+")))
                .groupBy((key, value) -> value)
                        .count();

        table.toStream()
                .peek((key, value) -> System.out.println(String.format("After aggregation:: %s - %s", key, value)))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

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
