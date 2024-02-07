package org.brs.test_task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class RabbitPublisherImpl implements TargetedRabbitPublisher {
    private static Logger logger = LoggerFactory.getLogger(RabbitPublisherImpl.class);

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
                    logger.info("Message returned successfully with code: {}", AMQP.REPLY_SUCCESS);
                    replyCodeFuture.complete(AMQP.REPLY_SUCCESS);
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) {
                    cleanupConfirms(deliveryTag, multiple);
                    logger.info("Message got negative acknowledgement, with code: {}", AMQP.INTERNAL_ERROR);
                    replyCodeFuture.complete(AMQP.INTERNAL_ERROR);
                }
            });

            channel.addReturnListener((replyCode, replyText, exchange, routingKey, returnProperties, returnBody) -> {
                logger.error("Message returned with code: {}, reason: {}", replyCode, replyText);
                replyCodeFuture.complete(replyCode);
            });

            channel.addShutdownListener((cause) -> {
                logger.error("Channel/connection closed. Reason: {}", cause.getReason());
                AMQP.Channel.Close closeReason = (AMQP.Channel.Close) cause.getReason();
                replyCodeFuture.complete(closeReason.getReplyCode());
            });

            outstandingConfirms.put(channel.getNextPublishSeqNo(), body);
            channel.basicPublish(exchangeName, queueName, props, body);
            return replyCodeFuture.get(timeoutSeconds, TimeUnit.SECONDS);

        } catch (UnknownHostException e) {
            logger.error(e.getMessage());
            return AMQP.INVALID_PATH;
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage());
            return AMQP.INTERNAL_ERROR;
        } finally {
            // Explicitly check and close resources
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException | TimeoutException e) {
                    logger.error("Error closing channel: {}", e.getMessage());
                }
            }
            if (connection != null && connection.isOpen()) {
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.error("Error closing connection: {}", e.getMessage());
                }
            }
        }
    }

    private void cleanupConfirms(long deliveryTag, boolean multiple) {
        logger.debug("Cleaning outstanding confirm for delivery tag: {}", deliveryTag);
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
                logger.error("Error processing property {}. Cause: {}", key, e.getMessage());
                throw new IOException("Error processing property " + key, e);
            } catch (JsonProcessingException e) {
                logger.error("Error parsing JSON for headers: {}", e.getMessage());
                throw new IOException("Error processing headers ", e);
            }
        };

        return builder.build();
    }
}
