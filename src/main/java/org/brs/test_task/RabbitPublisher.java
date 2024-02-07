package org.brs.test_task;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface RabbitPublisher {
    int publishMessageAndGetReplyCode(byte[] body, Map<String,String> properties) throws TimeoutException, IOException;
}
