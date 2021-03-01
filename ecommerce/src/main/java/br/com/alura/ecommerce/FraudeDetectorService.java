package br.com.alura.ecommerce;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
//CONSUMIDOR DE MENSAGENS
public class FraudeDetectorService {

	public static void main(String[] args) {
		var fraudService = new FraudeDetectorService();
		var service = new KafkaService<>(
				FraudeDetectorService.class.getSimpleName(),
				"ECOMMERCE_NEW_ORDER", 
				fraudService::parse,
				Order.class,
				Map.of());
				service.run();
		}
	
	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("-----------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		System.err.println(record.key());
		System.err.println(record.value());
		System.err.println(record.partition());
		System.err.println(record.offset());
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Order processed");
	}
		
//	private static Properties properties() {
//		var properties = new Properties();
//		//properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");//o local onde esse consumidor deve ficar escutando se existem novas mensagens criadas
//		//properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //a maneira que ele deve deserializar a chave da mensagem
//		//properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());//maneira que ele deve deserializar o valor da mensagem 
//		//properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudeDetectorService.class.getSimpleName());
//		// properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudeDetectorService.class.getName());
//		//properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");//informando ao kafka que ele deve fazer o commit/poll das mensagens de uma em uma, assim podemos evitar que o kafka faça um rebalanceamento antes de terminar de consumir as mensagens
//		return properties;
//	}
	
}
