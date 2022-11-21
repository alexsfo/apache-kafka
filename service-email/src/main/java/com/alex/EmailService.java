package com.alex;


import ch.qos.logback.classic.Level;
import com.alex.utils.LoggerUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService {

    public static void main(String[] args) {
        LoggerUtils.setLoggerLevel(Level.ERROR);

        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("---------------------------------------------------------------");
        System.out.println("Sending email");
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
        System.out.println("email sent.");
        System.out.println("---------------------------------------------------------------\n");
    }

}
