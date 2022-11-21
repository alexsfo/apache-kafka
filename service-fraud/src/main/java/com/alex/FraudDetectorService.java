package com.alex;


import ch.qos.logback.classic.Level;
import com.alex.utils.LoggerUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService {


    public static void main(String[] args) {
        LoggerUtils.setLoggerLevel(Level.ERROR);

        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("---------------------------------------------------------------");
        System.out.println("Processing new order. Checking for fraud.");
        System.out.println("topic: " + record.topic().toUpperCase());
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            //ignoring
            e.printStackTrace();
        }
        System.out.println("Order processed.");
        System.out.println("---------------------------------------------------------------\n");
    }

}
