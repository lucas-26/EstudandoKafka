package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFuction<T> {
	 void consumer(ConsumerRecord<String, T> record);
}
