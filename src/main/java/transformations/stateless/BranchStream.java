package transformations.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BranchStream {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder  builder = new StreamsBuilder();
        KStream<String, String>  source = builder.stream("branch-stream-input");

        Map<String, KStream<String, String>> branches = source.split(Named.as("branch-stream-"))
                .branch((key, value) -> value.toLowerCase().equals("kafka"), Branched.as("kafka"))
                .branch((key, value) -> value.toLowerCase().equals("scalable"), Branched.as("scalable"))
                .branch((key, value) -> value.toLowerCase().equals("distributable"), Branched.as("distributable"))
                .defaultBranch(Branched.as("general"));

        branches.get("branch-stream-kafka").to("branch-stream-kafka-output");
        branches.get("branch-stream-scalable").to("branch-stream-scalable-output");
        branches.get("branch-stream-distributable").to("branch-stream-distributable-output");
        branches.get("branch-stream-general").to("branch-stream-general-output");

        System.out.println("brancqhes");
        System.out.println(branches.toString());

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
