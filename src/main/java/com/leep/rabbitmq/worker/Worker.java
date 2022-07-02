package com.leep.rabbitmq.worker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Worker {

    public static void main(String[] argv) throws Exception {
        int TEST_Count = 3;
        for (int i = 0; i < TEST_Count; i++) {
            // Thread를 3개를 실행을 하므로 Consumer가 3개이다.
            new MyThread(i).start();
        }
    }

}

/**
 * Consumer - example2
 */
class MyThread extends Thread {
    private static final String TASK_QUEUE_NAME = "task_queue";

    private int num;

    MyThread(int num) {
        this.num = num;
    }

    @Override
    public void run() {
        int TEST_qos = 1;
        ConnectionFactory factory  = null;
        try {
            factory  = new ConnectionFactory();
            factory .setHost("localhost");
            factory .setPort(5672);
            factory .setUsername("admin");
            factory .setPassword("asdf!234");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        final Connection connection;
        try {
            connection = factory.newConnection();
            final Channel channel = connection.createChannel();

            // durable: 서버를 재시작 하더라도 메시지가 유실되지 않도록 하는 큐로 선언할 것인지 여부
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            channel.basicQos(TEST_qos);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                // doWork를 실행하기 전
                String message = new String(delivery.getBody(), "UTF-8");

                // num은 현재 작업을 진행하는 쓰레드의 번호이다.
                System.out.println(num + " [x] Received '" + message + "'");
                try {
                    // doWork 실행
                    doWork(message);
                } finally {
                    // doWork를 실행 한 후
                    System.out.println(num + " [x] Done");
                    // 데이터를 수령하는 코드
                    // channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            //Done을 출력하고 큐에게 확인 메세지를 보낸다. (데이터의 승인을 대한 신호를 큐에 알린다)
            channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> {
            });
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


        super.run();
    }

    /**
     * 별도의 출력은 하지않고 2초 동안 지연시키는 함수
     * @param task
     */
    private void doWork(String task) {
        int TEST_TIME = 200; // 작업에 소요되는 시간
        try {
            Thread.sleep(TEST_TIME);
        } catch (InterruptedException _ignored) {
            Thread.currentThread().interrupt();
        }
    }
}