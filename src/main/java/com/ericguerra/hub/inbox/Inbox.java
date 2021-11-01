package com.ericguerra.hub.inbox;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Clock;

/**
 * Exactly-once *effects* on top of Kafka's at-least-once delivery. Claims a message id inside the
 * caller's transaction: if the handler later fails, the claim rolls back with it and the redelivery
 * runs again; if it commits, every redelivery after that is a no-op.
 */
@Component
public class Inbox {
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public Inbox(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    /** @return true if this consumer has not processed the message before (caller should proceed). */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean claim(String consumer, String messageId) {
        try {
            jdbc.update("INSERT INTO inbox (consumer, message_id, processed_at) VALUES (?, ?, ?)",
                    consumer, messageId, Timestamp.from(clock.instant()));
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    @Transactional
    public int purgeOlderThanDays(int days) {
        // Must exceed the topic's retention, or a replay from the earliest offset could double-apply.
        return jdbc.update("DELETE FROM inbox WHERE processed_at < ?", Timestamp.from(clock.instant().minusSeconds(days * 86400L)));
    }
}
