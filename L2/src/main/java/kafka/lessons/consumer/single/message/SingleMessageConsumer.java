package kafka.lessons.consumer.single.message;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import java.io.IOException;
import java.util.Collections;
import kafka.lessons.Constants;
import kafka.lessons.pojo.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleMessageConsumer.class);

    private static final ConsumerSettings SETTINGS;

    private static final ObjectMapper MAPPER;

    static {
        SETTINGS = new ConsumerSettings();
        MAPPER = new CBORMapper();
    }

    public static void main(String[] args) {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(SETTINGS.getConsumerConfig())) {
            consumer.subscribe(Collections.singletonList(Constants.TOPIC));
            while (true) {
                try {
                    LOGGER.info("Start new iteration");
                    final ConsumerRecords<byte[], byte[]> records = consumer.poll(SETTINGS.getPollTimeout());
                    LOGGER.info("Received records count: {}", records.count());
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        final User user = MAPPER.readValue(record.value(), User.class);
                        LOGGER.info("Received user: {}", user);
                    }
                } catch (InvalidTopicException e) {
                    LOGGER.error("Setup invalid topic or topic doesn't exists now", e);
                } catch (JacksonException e) {
                    LOGGER.error("Deserialization error for consumer record. Skipping record", e);
                } catch (IOException | KafkaException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
