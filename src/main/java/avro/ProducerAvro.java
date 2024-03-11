/**
 * 
 */
package avro;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import avro.pojo.TruckPojo;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * 
 */
public class ProducerAvro {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
		props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		props.setProperty("schema.registry.url", "http://localhost:8081");
		
		try(KafkaProducer<Integer, TruckPojo> producer = new KafkaProducer<>(props)) {
			TruckPojo truck = new TruckPojo(2, "21.45N", "85.56E");
			ProducerRecord<Integer, TruckPojo> recordPojo = new ProducerRecord<>("TruckAvroTopic", 1, truck);
			producer.send(recordPojo);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
