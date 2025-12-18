package kafka.lessons.pojo;

import java.util.UUID;

public record User(UUID id, String name, int age) {}
