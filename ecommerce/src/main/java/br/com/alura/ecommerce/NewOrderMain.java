package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

//PRODUTOR DE MENSAGENS
public class NewOrderMain { // aqui está meu produtor de mensagens, ele em um ecommerce por exemplo, seria o
							// que cria um novo pedido que vou requisitado pelo usuário para quem está
							// escutando

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		try (var dispacher = new KafkaDispacher<Order>()) {
			var emaildispacher = new KafkaDispacher<String>();
			for (var i = 0; i < 10; i++) {

				var userId = UUID.randomUUID().toString();
				var orderId = UUID.randomUUID().toString();
				var amount = Math.random() * 5000 + 1;
				var newamaount = new BigDecimal(amount);
				var order = new Order(userId, orderId, newamaount);

				dispacher.send("ECOMMERCE_NEW_ORDER", userId, order);

				var email = "Thanks you for your order!";
				emaildispacher.send("ECOMMERCE_SEND_EMAIL", userId, email);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
