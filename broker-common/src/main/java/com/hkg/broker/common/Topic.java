package com.hkg.broker.common;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A topic name. Validated to a conservative character set (letters, digits,
 * dot, underscore, hyphen) matching Kafka's own topic-name rules.
 */
public record Topic(String name) {

    private static final Pattern LEGAL = Pattern.compile("[A-Za-z0-9._-]{1,249}");

    public Topic {
        Objects.requireNonNull(name, "topic name");
        if (!LEGAL.matcher(name).matches()) {
            throw new IllegalArgumentException("illegal topic name: " + name);
        }
    }
}
