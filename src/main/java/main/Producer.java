/**
 * 
 */
package main;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import dto.Order;
import serializer.OrderSerilaizer;

/**
 * 
 */
public class Producer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerilaizer.class.getName());

		try (KafkaProducer<Integer, Order> producer = new KafkaProducer<>(props);) {
			ProducerRecord<Integer, Order> recordVal1 = new ProducerRecord<>("first-topic", 1, new Order(1, "20.42N", "88.56E"));
			ProducerRecord<Integer, Order> recordVal2 = new ProducerRecord<>("first-topic", 2, new Order(2, "18.42N", "87.56E"));
			ProducerRecord<Integer, Order> recordVal3 = new ProducerRecord<>("first-topic", 3, new Order(3, "23.42N", "86.56E"));
			ProducerRecord<Integer, Order> recordVal4 = new ProducerRecord<>("first-topic", 4, new Order(4, "16.42N", "89.56E"));
			ProducerRecord<Integer, Order> recordVal5 = new ProducerRecord<>("first-topic", 5, new Order(5, "21.42N", "85.56E"));
			
			producer.send(recordVal1);
			producer.send(recordVal2);
			producer.send(recordVal3);
			producer.send(recordVal4);
			producer.send(recordVal5);
			System.out.println("Messages sent");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
