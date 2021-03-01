package br.com.alura.ecommerce;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaDispacher<T> implements Closeable{

	private KafkaProducer<String, T> producer;


	public KafkaDispacher() {

		this.producer = new KafkaProducer<>(properties()); //configurando o producer que eu irei enviar, meu producer sempre terá chave e valor
	}

	
	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG  ,"127.0.0.1:9092");//informando onde estão rodando os meus kafkas
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //informando como a chave do meu producer vai ser serializada
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName()); //informando como o valor do meu producer vai ser serializada
		return properties;
	}


	public void send(String topic, String key, T value) throws InterruptedException, ExecutionException {
		var record = new ProducerRecord<>(topic , key, value); //a chave "key" é a maneira que o kafka utiliza pra distribuir as mensagens entre as partições que lidam com esse grupo.		
		Callback callback = (data, ex) -> {
			if (ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println(data.topic() +":::" + data.partition() + "/" + data.offset() +"/" + data.timestamp());
		};
		producer.send(record, callback).get();
		
	}


	@Override
	public void close() throws IOException {
			producer.close();
	}
}
