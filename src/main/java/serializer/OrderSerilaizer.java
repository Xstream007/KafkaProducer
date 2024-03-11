/**
 * 
 */
package serializer;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import dto.Order;

/**
 * 
 */
public class OrderSerilaizer implements Serializer<Order> {

	@Override
	public byte[] serialize(String topic, Order data) {
		byte[] response = null;
		ObjectMapper mapper = new ObjectMapper();

		try {
			response = mapper.writeValueAsBytes(data);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return response;
	}

}
