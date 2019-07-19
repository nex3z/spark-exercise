package com.nex3z.examples.spark.stream.passengercount;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class PassengerProducer {
    private static final String SERVERS = "localhost:9092";
    private static final String TOPIC = "Passenger";

    private static final Random random = new Random();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        Producer<String, String> producer = new KafkaProducer<>(props);

        while (true) {
            Passenger passenger = buildRandomPassenger("SHOP_A");
            String key = buildKey(passenger);
            producer.send(new ProducerRecord<>(TOPIC, key, JSON.toJSONString(passenger)));

            delay();

            passenger = buildRandomPassenger("SHOP_B");
            key = buildKey(passenger);
            producer.send(new ProducerRecord<>(TOPIC, key, JSON.toJSONString(passenger)));

            delay();
        }
    }

    private static Passenger buildRandomPassenger(String shopName) {
        return Passenger.builder()
                .shopName(shopName)
                .passengerId(UUID.randomUUID().toString())
                .age(random.nextInt(30))
                .arriveTime(formatDate(new Date()))
                .build();
    }

    private static String buildKey(Passenger passenger) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        return formatter.format(new Date()) + "-" + passenger.getShopName();
    }

    public static String formatDate(Date date) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        return formatter.format(date);
    }

    private static void delay() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
