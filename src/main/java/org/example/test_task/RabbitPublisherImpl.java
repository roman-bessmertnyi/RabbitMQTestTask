package org.example.test_task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.*;

public class RabbitPublisherImpl implements TargetedRabbitPublisher {
    private final ObjectMapper objectMapper;
    private final DateTimeFormatter dateFormat;
    private final int timeoutSeconds;
    private String queueName;
    private String exchangeName;
    private final ConnectionFactory factory;
    private ConcurrentNavigableMap<Long, byte[]> outstandingConfirms;

    public RabbitPublisherImpl(ConnectionFactory factory, String dateFormat, int timeoutSeconds) {
        this.factory = factory;
        this.dateFormat = DateTimeFormatter.ofPattern(dateFormat);
        this.timeoutSeconds = timeoutSeconds;
        this.objectMapper = new ObjectMapper();
        this.queueName = "";
        this.exchangeName = "";
        outstandingConfirms = new ConcurrentSkipListMap<>();
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getExchangeName() {
        return this.exchangeName;
    }

    public String getQueueName() {
        return this.queueName;
    }

    @Override
    public int publishMessageAndGetReplyCode(byte[] body, Map<String, String> properties) throws TimeoutException, IOException {
        AMQP.BasicProperties props = buildProperties(properties);
        final CompletableFuture<Integer> replyCodeFuture = new CompletableFuture<>();

        Connection connection = null;
        Channel channel = null;
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.confirmSelect();

            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) {
                    cleanupConfirms(deliveryTag, multiple);
                    replyCodeFuture.complete(AMQP.REPLY_SUCCESS);
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) {
                    cleanupConfirms(deliveryTag, multiple);
                    replyCodeFuture.complete(AMQP.INTERNAL_ERROR);
                }
            });

            channel.addReturnListener((replyCode, replyText, exchange, routingKey, returnProperties, returnBody) -> {
                System.out.println("Message returned with code: " + replyCode + " reason: " + replyText);
                replyCodeFuture.complete(replyCode);
            });

            channel.addShutdownListener((cause) -> {
                System.out.println("Channel/connection closed. Reason: " + cause.getReason());
                AMQP.Channel.Close closeReason = (AMQP.Channel.Close) cause.getReason();
                replyCodeFuture.complete(closeReason.getReplyCode());
            });

            outstandingConfirms.put(channel.getNextPublishSeqNo(), body);
            channel.basicPublish(exchangeName, queueName, props, body);
            return replyCodeFuture.get(20, TimeUnit.SECONDS);

        } catch (UnknownHostException e) {
            return AMQP.INVALID_PATH;
        } catch (InterruptedException | ExecutionException e) {
            return AMQP.INTERNAL_ERROR;
        } finally {
            // Explicitly check and close resources
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException | TimeoutException e) {
                    System.err.println("Error closing channel: " + e.getMessage());
                }
            }
            if (connection != null && connection.isOpen()) {
                try {
                    connection.close();
                } catch (IOException e) {
                    System.err.println("Error closing connection: " + e.getMessage());
                }
            }
        }
    }

    private void cleanupConfirms(long deliveryTag, boolean multiple) {
        System.out.println("Cleaning outstanding confirm for delivery tag: " + deliveryTag);
        if (multiple) {
            ConcurrentNavigableMap<Long, byte[]> confirmed = outstandingConfirms.headMap(
                    deliveryTag, true
            );
            confirmed.clear();
        } else {
            outstandingConfirms.remove(deliveryTag);
        }
    }

    private AMQP.BasicProperties buildProperties(Map<String, String> propertiesMap) throws IOException {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

        if (propertiesMap == null) {
            return builder.build();
        }

        for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            try {
                switch (key) {
                    case "deliveryMode":
                        builder.deliveryMode(Integer.parseInt(value));
                        break;
                    case "expiration":
                        builder.expiration(value);
                        break;
                    case "priority":
                        builder.priority(Integer.parseInt(value));
                        break;
                    case "userId":
                        builder.userId(value);
                        break;
                    case "appId":
                        builder.appId(value);
                        break;
                    case "contentEncoding":
                        builder.contentEncoding(value);
                        break;
                    case "contentType":
                        builder.contentType(value);
                        break;
                    case "correlationId":
                        builder.correlationId(value);
                        break;
                    case "messageId":
                        builder.messageId(value);
                        break;
                    case "replyTo":
                        builder.replyTo(value);
                        break;
                    case "timestamp":
                        Date date = (Date) dateFormat.parse(value);
                        builder.timestamp(date);
                        break;
                    case "type":
                        builder.type(value);
                        break;
                    case "headers":
                        Map<String, Object> headers = objectMapper.readValue(value, Map.class);
                        builder.headers(headers);
                        break;
                }
            } catch (NumberFormatException |
                     DateTimeParseException e) {
                throw new IOException("Error processing property " + key, e);
            } catch (JsonProcessingException e) {
                System.err.println("Error parsing JSON for headers: " + e.getMessage());
            }
        };

        return builder.build();
    }
}
