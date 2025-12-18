package kafka.lessons.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.google.common.util.concurrent.RateLimiter;
import kafka.lessons.Constants;
import kafka.lessons.pojo.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.instancio.Instancio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private static final ObjectMapper MAPPER;

    private static final ProducerSettings SETTINGS;

    static {
        SETTINGS = ProducerSettings.create();
        LOGGER.info("Setting up producer application. Settings: {}", SETTINGS);
        MAPPER = new CBORMapper();
    }

    public static void main(String[] args) throws JsonProcessingException {
        final RateLimiter limiter = RateLimiter.create(SETTINGS.getRps());

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(SETTINGS.getProducerConfig())) {
            for (int i = 0; i < SETTINGS.getRecordsCount(); i++) {
                limiter.acquire();

                final User user = Instancio.create(User.class);
                final byte[] value = MAPPER.writeValueAsBytes(user);
                final byte[] key = MAPPER.writeValueAsBytes(user.id());

                final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(Constants.TOPIC, key, value);

                producer.send(record, (metadata, exception) -> {
                    // Тут могла бы быть более продвинутая система реагирования на ошибки публичного API
                    // В рамках этого задания оставляем просто вывод ошибок в лог
                    if (exception != null) {
                        LOGGER.error("Error sending record", exception);
                        return;
                    }
                    LOGGER.info("Sent record into '{}' topic, '{}' partition", metadata.topic(), metadata.partition());
                });
            }
        }
    }
}