package com.alex;


import ch.qos.logback.classic.Level;
import com.alex.utils.LoggerUtils;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        LoggerUtils.setLoggerLevel(Level.ERROR);
        var logService = new LogService();

        try (var service = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }
    }

    @SneakyThrows
    private void parse(ConsumerRecord<String, String> record) {
        Thread.sleep(1000l);
        System.out.println("LOG---> topic: " + record.topic().toUpperCase());
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        System.out.println("--------------------------------------------------------------- \n");
    }


}
