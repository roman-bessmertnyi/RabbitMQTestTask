package org.example.test_task;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitPublisherTester {
    private final static String QUEUE_NAME = "rabbit_publisher_1";
    private final static String HOST = "fake_host";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        RabbitPublisher publisher = new RabbitPublisherImpl(QUEUE_NAME, factory);

        String message = "Hello World!";
        int responseCode = publisher.publishMessageAndGetReplyCode(message.getBytes(), null);

        System.out.println("Sent message, return code is: " + responseCode);
    }
}
