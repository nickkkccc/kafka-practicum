package kafka.lessons.consumer.batch;

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

public class BatchConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchConsumer.class);

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
                    // во избежания холостых запросов `poll timeout >> fetch.max.wait.ms`
                    final ConsumerRecords<byte[], byte[]> records = consumer.poll(SETTINGS.getPollTimeout());
                    LOGGER.info("Received records count: {}", records.count());

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        final User user = MAPPER.readValue(record.value(), User.class);
                        LOGGER.info("Received user: {}", user);
                    }
                    // коммитим все записи в батче тк неверные мы просто игнорируем и выводим сообщения в лог
                    // (объяснение ниже)
                    // хотя мы могли бы здесь падать fail-fast, если бы последовательность в партиции, например, для
                    // нашей системы имела значения и пропуски значений являются критичными
                    consumer.commitSync(records.nextOffsets());
                } catch (InvalidTopicException e) {
                    LOGGER.error("Setup invalid topic or topic doesn't exists now", e);
                } catch (JacksonException e) {
                    // Здесь мы могли бы несериализуемые записи оправлять, например, в DLQ, в рамках задания не
                    // указано, что делать с поломанным батчем или записью, поэтому мы просто игнорируем таковые
                    LOGGER.error("Deserialization error for consumer record. Skipping record", e);
                } catch (IOException | KafkaException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
