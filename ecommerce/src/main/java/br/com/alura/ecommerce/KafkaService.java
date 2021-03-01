package br.com.alura.ecommerce;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaService<T> {

	private KafkaConsumer<String, T> consumer;
	private final ConsumerFuction parse;
	
	public KafkaService(String groupId, Pattern topic, ConsumerFuction parse ,Class<T> type, Map<String, String> map) {
		this(parse, groupId, type, map);
		//this.consumer = new KafkaConsumer<String, T>(properties(type, groupId));//criando um consumidor de mensagens e chamando o método que configura ele 
		consumer.subscribe(topic); //falando qual criador de mensagem ele deve ficar escutando
	}
	
	public KafkaService(String groupId, String topic, ConsumerFuction parse, Class<T> type, Map<String, String> map) {
		this(parse, groupId, type, map);
		//this.consumer = new KafkaConsumer<String, T>(properties(null , groupId));//criando um consumidor de mensagens e chamando o método que configura ele 
		 //falando qual criador de mensagem ele deve ficar escutando 
		consumer.subscribe(Collections.singleton(topic));
	}
	
	public KafkaService(ConsumerFuction parse, String groupId, Class<T> type, Map<String, String> map) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<>(properties(type, groupId, map));
	}

	public void run() {
		while (true) {
			var records = consumer.poll(Duration.ofMillis(100));//de quanto em quanto tempo ele deve ficar escutando esse endereço, vai retornar várias mensagens
			if(!records.isEmpty()) {
				System.out.println("Encontrei" + records.count() + "registros");
				for(var record: records){
					parse.consumer(record);
				};
			}
		}
	}
	
	private Properties properties(Class<T> type, String groupId, Map<String, String> map) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");//o local onde esse consumidor deve ficar escutando se existem novas mensagens criadas
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //a maneira que ele deve deserializar a chave da mensagem
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());//maneira que ele deve deserializar o valor da mensagem 
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
		properties.putAll(map);
		return properties;
	}

}
