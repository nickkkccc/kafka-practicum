package kafka.lessons.consumer;

import java.time.Duration;
import java.util.Map;

public abstract class AbstractConsumerSettings {

    protected static final String POLL_TIMEOUT = "poll.timeout";
    protected static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(10);

    private final Map<String, Object> settings;

    private final Duration pollTimeout;

    public AbstractConsumerSettings(Map<String, Object> settings) {
        final String rawPollTimeout = System.getenv().get(POLL_TIMEOUT);
        this.pollTimeout = rawPollTimeout == null ? DEFAULT_POLL_TIMEOUT : Duration.parse(rawPollTimeout);
        this.settings = settings;
    }

    public Duration getPollTimeout() {
        return this.pollTimeout;
    }

    public Map<String, Object> getConsumerConfig() {
        return this.settings;
    }
}
