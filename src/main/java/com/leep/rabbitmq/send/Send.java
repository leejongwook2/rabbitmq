package com.leep.rabbitmq.send;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Producer - example1
 */
@Component
public class Send {

    private static final String QUEUE_NAME ="rabbit";

    public static void main(String[] args) {
        System.out.println("start");

        // CoonectionFactory를 위한 connection을 생성한다.
        ConnectionFactory factory = null;
        try {
            factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(5672);
            factory.setUsername("admin");
            factory.setPassword("asdf!234");
//            factory.setAutomaticRecoveryEnabled(true);
//            factory.setShutdownTimeout(10000);
//            factory.setRequestedHeartbeat(60);
//            factory.setVirtualHost("/");
//            factory.isSSL();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        /**
         * 컨넥션을 생성하고 Channel을 만든다.
         * Connection connection = factory.newConnection();
         * Channel channel = connection.createChannel()
         *
         * Connection - Application과 RabbitMQ Broker 사이의 TCP 연결
         * Channel - connection 내부에 정의된 가상의 연결. queue에서 데이터를 손볼 때 생기는 일종의 통로같은 개념
         *
         * basicPublish중에 첫번째 파라메터를 빈문자열을 넘겨주는데 이는 exchange이다.
         * 그런데 이번 구조에서는 exchange가 필요가 없다.
         * 빈문자열을 넘겨준다는건 기본 exchange를 사용하겠다는 뜻이다.
         */
        try {
            System.out.println(factory);
            Connection connection = factory.newConnection();
            System.out.println(connection);
            System.out.println(connection);
            System.out.println(connection);
            System.out.println(connection);
            Channel channel = connection.createChannel();
            for (int i = 0; i <= 1; i++) {
                channel.queueDeclare(QUEUE_NAME, false, false, false, null); // channel 에 queue를 생성
                String message = "Hello World!" + (int) (Math.random() * 100);
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes()); // 메시지 전송
                System.out.println(" [x] Set '" + message + "'");
                Thread.sleep(10);
            }
        } catch (TimeoutException e) {
            System.out.println("1");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
