package org.example.test_task;

public interface TargetedRabbitPublisher extends RabbitPublisher{
    public void setExchangeName(String exchangeName);

    public void setQueueName(String queueName);

    public String getExchangeName();

    public String getQueueName();
}
