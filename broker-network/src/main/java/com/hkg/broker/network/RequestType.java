package com.hkg.broker.network;

/**
 * Broker request type discriminator. Kept deliberately small — the goal is
 * to demonstrate the framing + dispatch pattern, not to mirror Kafka's
 * full API surface.
 */
public enum RequestType {
    PRODUCE,
    FETCH,
    METADATA
}
