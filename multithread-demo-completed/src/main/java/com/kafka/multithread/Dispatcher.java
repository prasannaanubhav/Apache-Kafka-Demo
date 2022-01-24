package com.kafka.multithread;

import java.io.File;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Dispatcher implements Runnable {

	private static final Logger logger = LogManager.getLogger();

	private KafkaProducer<Integer, String> producer;
	private String fileLocation;
	private String topicName;

	public Dispatcher() {

	}

	public Dispatcher(KafkaProducer<Integer, String> producer, String fileLocation, String topicName) {

		this.producer = producer;
		this.fileLocation = fileLocation;
		this.topicName = topicName;
	}

	@Override
	public void run() {

		logger.info("start processing " + fileLocation);

		File file = new File(fileLocation);

		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {

				String data = scanner.nextLine();

				producer.send(new ProducerRecord<Integer, String>(topicName, data));

			}

			logger.info("Finishing sending file");
		} catch (Exception e) {

		}

		finally {
			producer.close();
		}

	}

}
