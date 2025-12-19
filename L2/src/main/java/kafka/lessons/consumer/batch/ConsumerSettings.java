package kafka.lessons.consumer.batch;

import java.util.Map;
import java.util.UUID;
import kafka.lessons.Constants;
import kafka.lessons.consumer.AbstractConsumerSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class ConsumerSettings extends AbstractConsumerSettings {

    public ConsumerSettings() {
        super(createConsumerConfig());
    }

    private static Map<String, Object> createConsumerConfig() {
        final String bootstrapServers = System.getenv()
                .getOrDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.DEFAULT_BOOTSTRAP_SERVERS);
        final String groupId = System.getenv()
                .getOrDefault(ConsumerConfig.GROUP_ID_CONFIG, "consumer-" + UUID.randomUUID());
        final String rawFetchMaxWaitMs = System.getenv(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        final String rawFetchMinBytes = System.getenv(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);

        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,

                ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                rawFetchMinBytes == null ? ConsumerConfig.DEFAULT_FETCH_MIN_BYTES :
                        Integer.parseUnsignedInt(rawFetchMinBytes),
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                rawFetchMaxWaitMs == null ? ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS :
                        Integer.parseUnsignedInt(rawFetchMaxWaitMs),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
    }
}
