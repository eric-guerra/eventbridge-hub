package com.ericguerra.hub.events;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

/**
 * What Debezium's outbox EventRouter produces: key = aggregate_id, value = payload, and the outbox
 * row id / event type as headers (see connect/order-outbox-connector.json).
 */
public record EventEnvelope(String id, String type, String aggregateId, String payload) {

    public static EventEnvelope from(ConsumerRecord<String, String> rec) {
        String id = header(rec, "id");
        String type = header(rec, "eventType");
        if (id == null || type == null) throw new MalformedEventException("missing id/eventType headers at " + rec.topic() + "@" + rec.offset());
        if (rec.value() == null) throw new MalformedEventException("tombstone / empty payload for " + id);
        return new EventEnvelope(id, type, rec.key(), rec.value());
    }

    private static String header(ConsumerRecord<?, ?> rec, String name) {
        Header h = rec.headers().lastHeader(name);
        if (h == null) return null;
        String v = new String(h.value(), StandardCharsets.UTF_8);
        // Debezium JSON-encodes header values when using the JSON converter: "\"abc\"" → abc
        return v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"") ? v.substring(1, v.length() - 1) : v;
    }

    /** Never retryable: goes straight to the dead-letter topic. */
    public static class MalformedEventException extends RuntimeException {
        public MalformedEventException(String m) { super(m); }
        public MalformedEventException(String m, Throwable cause) { super(m, cause); }
    }
}
