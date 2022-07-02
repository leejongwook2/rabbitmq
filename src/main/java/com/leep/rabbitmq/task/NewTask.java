package com.leep.rabbitmq.task;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * Producer - example2
 */
public class NewTask {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        int TEST_MESSAGES = 8;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("asdf!234");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            for (int i = 1; i <= TEST_MESSAGES; i++) {
                // durable: 서버를 재시작 하더라도 메시지가 유실되지 않도록 하는 큐로 선언할 것인지 여부
                channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
                String message = "Hello World!" + i;
                channel.basicPublish("", TASK_QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }
        }
    }

}
