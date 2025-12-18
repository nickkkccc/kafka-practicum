package kafka.lessons.producer;

import java.util.Map;
import kafka.lessons.Constants;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ProducerSettings {

    private static final String RECORDS_COUNT_CONFIG = "records.count";
    private static final int DEFAULT_RECORDS_COUNT = 100;

    private static final String RPS_CONFIG = "rps";
    private static final double DEFAULT_RPS = 0.5;

    private final double rps;
    private final int recordsCount;
    private final Map<String, Object> producerConfig;

    private ProducerSettings(double rps, int recordsCount, Map<String, Object> producerConfig) {
        this.rps = rps;
        this.recordsCount = recordsCount;
        this.producerConfig = producerConfig;
    }

    public static ProducerSettings create() {
        final String rawRps = System.getenv(RPS_CONFIG);
        final double rps = rawRps == null ? DEFAULT_RPS : Double.parseDouble(rawRps);

        final String rawRecordsCount = System.getenv(RECORDS_COUNT_CONFIG);
        final int recordsCount =
                rawRecordsCount == null ? DEFAULT_RECORDS_COUNT : Integer.parseUnsignedInt(rawRecordsCount);

        return new ProducerSettings(rps, recordsCount, resolveProducerConfig());
    }

    private static Map<String, Object> resolveProducerConfig() {
        final String bootstrapServers = System.getenv().getOrDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Constants.DEFAULT_BOOTSTRAP_SERVERS);
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()
        );
    }

    public double getRps() {
        return this.rps;
    }

    public int getRecordsCount() {
        return this.recordsCount;
    }

    public Map<String, Object> getProducerConfig() {
        return this.producerConfig;
    }

    @Override
    public String toString() {
        return "Settings{rps=" + rps + ", recordsCount=" + recordsCount + '}';
    }
}
