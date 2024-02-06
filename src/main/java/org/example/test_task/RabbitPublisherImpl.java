package org.example.test_task;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.*;

public class RabbitPublisherImpl implements RabbitPublisher {
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private SimpleDateFormat dateFormat;
    private String queue_name;
    private ConnectionFactory factory;

    public RabbitPublisherImpl(String queue_name, ConnectionFactory factory) {
        this.queue_name = queue_name;
        this.factory = factory;
        this.dateFormat = new SimpleDateFormat(DATE_FORMAT);
    }

    @Override
    public int publishMessageAndGetReplyCode(byte[] body, Map<String, String> properties) throws TimeoutException, IOException {



        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        properties.forEach(builder::header); // Assuming these are headers for simplicity
        AMQP.BasicProperties props = builder.build();
        AMQP.BasicProperties props = MessageProperties.PERSISTENT_TEXT_PLAIN;

        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            channel.confirmSelect();
            channel.basicPublish("", queue_name, props, body);
            if (channel.waitForConfirms()) {
                return 200; // Assuming 200 for success
            } else {
                return 312; // Example failure code, choose appropriate codes
            }
        } catch (IOException | InterruptedException e) {
            // Handle exception, possibly returning different codes for different exceptions
            return 500; // Example error code
        }
    }

    @FunctionalInterface
    interface PropertySetter {
        void apply(AMQP.BasicProperties.Builder builder, Object value);
    }

    Map<String, PropertySetter> propertyHandlers = Map.of(
            "deliveryMode", (builder, value) -> builder.deliveryMode(Integer.parseInt((String) value)),
            "expiration", (builder, value) -> builder.expiration((String) value),
            "priority", (builder, value) -> builder.priority(Integer.parseInt((String) value)),
            "userId", (builder, value) -> builder.userId((String) value),
            "appId", (builder, value) -> builder.appId((String) value),
            "contentEncoding", (builder, value) -> builder.contentEncoding((String) value),
            "contentType", (builder, value) -> builder.contentType((String) value),
            "correlationId", (builder, value) -> builder.correlationId((String) value),
            "messageId", (builder, value) -> builder.messageId((String) value),
            "timestamp", (builder, value) -> {
                try {
                    Date date = dateFormat.parse((String) value);
                    builder.timestamp(date);
                } catch (ParseException e) {
                    builder.timestamp(new Date());
                }
            },
            "replyTo", (builder, value) -> builder.replyTo((String) value)
            // Add other properties as necessary
    );

    private AMQP.BasicProperties buildProperties(Map<String, String> propertiesMap) {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

        if (propertiesMap.containsKey("deliveryMode")) {
            builder.deliveryMode(Integer.parseInt(propertiesMap.get("deliveryMode")));
        }
        if (propertiesMap.containsKey("expiration")) {
            builder.expiration(propertiesMap.get("expiration"));
        }
        if (propertiesMap.containsKey("priority")) {
            builder.priority(Integer.parseInt(propertiesMap.get("priority")));
        }
        if (propertiesMap.containsKey("userId")) {
            builder.userId(propertiesMap.get("userId"));
        }
        if (propertiesMap.containsKey("appId")) {
            builder.appId(propertiesMap.get("appId"));
        }
        if (propertiesMap.containsKey("contentEncoding")) {
            builder.contentEncoding(propertiesMap.get("contentEncoding"));
        }
        if (propertiesMap.containsKey("contentType")) {
            builder.contentType(propertiesMap.get("contentType"));
        }
        if (propertiesMap.containsKey("deliveryMode")) {
            builder.deliveryMode(Integer.parseInt(propertiesMap.get("deliveryMode")));
        }
        // Continue for other properties as needed...

        return builder.build();
    }
}
