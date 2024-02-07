import com.rabbitmq.client.ConnectionFactory;
import org.example.test_task.LargeStringGenerator;
import org.example.test_task.RabbitPublisherImpl;
import org.example.test_task.TargetedRabbitPublisher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RabbitPublisherTest {
    private static final int TIMEOUT_SECONDS = 20;
    private static final String EXCHANGE_NAME = "";
    private static final String FAKE_EXCHANGE_NAME = "fake_exchange";
    private static final String SECURE_EXCHANGE_NAME = "secure_exchange";
    private static final String QUEUE_NAME = "rabbit_publisher_1";
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String FAKE_HOST = "fakehost";
    private static final String HOST = "localhost";
    private static final String NORMAL_MESSAGE = "Hello World!";
    private ConnectionFactory factory;
    private TargetedRabbitPublisher publisher;

    @BeforeEach
    public void setup() {
        factory = new ConnectionFactory();
        factory.setHost(HOST);
        publisher = new RabbitPublisherImpl(factory, DATE_FORMAT, TIMEOUT_SECONDS);
        publisher.setExchangeName(EXCHANGE_NAME);
        publisher.setQueueName(QUEUE_NAME);
    }

    @Test
    public void testPublishMessageAndGetReplyCodeWithNormalMessage() throws Exception {
        int responseCode = publisher.publishMessageAndGetReplyCode(NORMAL_MESSAGE.getBytes(), null);
        Assertions.assertEquals(200, responseCode); // Assuming 200 is success code
    }

    @Test
    public void testPublishMessageAndGetReplyCodeWithFakeExchange() throws Exception {
        publisher.setExchangeName(FAKE_EXCHANGE_NAME);

        int responseCode = publisher.publishMessageAndGetReplyCode(NORMAL_MESSAGE.getBytes(), null);
        //not-found	404
        // channel
        // The client attempted to work with a server entity that does not exist.
        Assertions.assertEquals(404, responseCode); // Assuming 200 is success code
    }

    @Test
    public void testPublishMessageAndGetReplyCodeWithSecureExchange() throws Exception {
        publisher.setExchangeName(SECURE_EXCHANGE_NAME);

        int responseCode = publisher.publishMessageAndGetReplyCode(NORMAL_MESSAGE.getBytes(), null);
        //not-found	404
        // channel
        // The client attempted to work with a server entity that does not exist.
        Assertions.assertEquals(404, responseCode); // Assuming 200 is success code
    }

    @Test
    public void testPublishMessageAndGetReplyCodeWithFakeHost() throws Exception {
        factory.setHost(FAKE_HOST);

        int responseCode = publisher.publishMessageAndGetReplyCode(NORMAL_MESSAGE.getBytes(), null);
        //invalid-path
        // connection
        // The client tried to work with an unknown virtual host.
        Assertions.assertEquals(402, responseCode); // Assuming 200 is success code
    }

    @Test
    public void testPublishMessageAndGetReplyCodeWithExtraLargeMessage() throws Exception {
        String absurdlyLongMessage = LargeStringGenerator.generateLargeString(256);
        int responseCode = publisher.publishMessageAndGetReplyCode(absurdlyLongMessage.getBytes(), null);
        //precondition-failed
        // channel
        // The client requested a method that was not allowed because some precondition failed.
        Assertions.assertEquals(406, responseCode); // Assuming 200 is success code
    }
}
