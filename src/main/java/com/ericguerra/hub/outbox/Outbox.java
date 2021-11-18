package com.ericguerra.hub.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.UUID;

/**
 * Append an event to the outbox. MANDATORY propagation: calling this outside the business
 * transaction is a bug (the event could be published for a change that rolled back), so it fails loudly.
 */
@Component
public class Outbox {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final Clock clock;

    public Outbox(JdbcTemplate jdbc, ObjectMapper json, Clock clock) {
        this.jdbc = jdbc;
        this.json = json;
        this.clock = clock;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID append(String aggregateType, String aggregateId, String eventType, Object payload) {
        UUID id = UUID.randomUUID();
        try {
            jdbc.update("INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    id, aggregateType, aggregateId, eventType, json.writeValueAsString(payload), Timestamp.from(clock.instant()));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("payload not serializable", e);
        }
        return id;
    }

    /**
     * Debezium only needs the INSERT in the WAL; the row itself can go right away. A daily job keeps
     * a short tail for debugging instead of deleting inline (which doubles WAL traffic).
     */
    @Transactional
    public int purgeOlderThanHours(int hours) {
        return jdbc.update("DELETE FROM outbox WHERE created_at < ?", Timestamp.from(clock.instant().minusSeconds(hours * 3600L)));
    }
}
