package com.kafka.multithread;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DispatcherMain {

	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) throws IOException {

		Properties props = new Properties();

		InputStream inputStream = new FileInputStream(AppConfig.kafkaproperties);

		props.load(inputStream);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

		Thread[] thread = new Thread[AppConfig.filesLocation.length];

		logger.info("stating dispatcher thread....");

		for (int i = 0; i < AppConfig.filesLocation.length; i++) {

			thread[i] = new Thread(new Dispatcher(kafkaProducer, AppConfig.filesLocation[i], AppConfig.topicName));
			thread[i].start();

		}

		try {
			for (Thread t : thread) {
				t.join();
			}
		} catch (Exception e) {

		}

		finally {
			kafkaProducer.close();
			logger.info("finisher dispatcher demo");
		}

	}

}
