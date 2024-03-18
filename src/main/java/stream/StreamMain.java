/**
 * 
 */
package stream;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

/**
 * 
 */
public class StreamMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dataflow");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> streams = builder.stream("streams-dataflow-inputs");

		streams.foreach((key, value) -> System.out.println("Key - " + key + ", Value - " + value));

		streams.filter((key, value) -> value.contains("token"))
				.map((key, value) -> KeyValue.pair(key, value.toUpperCase())).to("streams-dataflow-output");

		Topology topology = builder.build();
		System.out.println("Topology description - " + topology.describe());

		KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

}
