package com.ericguerra.hub.orders;

import com.ericguerra.hub.outbox.Outbox;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

@Service
public class OrderService {
    public record Line(String sku, int qty) {}
    public record OrderPlaced(String orderId, String customerId, long totalCents, List<Line> lines, Instant placedAt) {}
    public record OrderCancelled(String orderId, String reason) {}

    private final JdbcTemplate jdbc;
    private final Outbox outbox;
    private final Clock clock;

    public OrderService(JdbcTemplate jdbc, Outbox outbox, Clock clock) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public UUID place(String customerId, long totalCents, List<Line> lines) {
        if (lines.isEmpty()) throw new IllegalArgumentException("order needs lines");
        UUID id = UUID.randomUUID();
        Instant now = clock.instant();
        jdbc.update("INSERT INTO orders (id, customer_id, status, total_cents, created_at) VALUES (?, ?, 'PLACED', ?, ?)",
                id, customerId, totalCents, Timestamp.from(now));
        for (Line l : lines) jdbc.update("INSERT INTO order_lines (order_id, sku, qty) VALUES (?, ?, ?)", id, l.sku(), l.qty());
        outbox.append("order", id.toString(), "OrderPlaced", new OrderPlaced(id.toString(), customerId, totalCents, lines, now));
        return id;
    }

    @Transactional
    public void cancel(UUID id, String reason) {
        int n = jdbc.update("UPDATE orders SET status = 'CANCELLED', version = version + 1 WHERE id = ? AND status <> 'CANCELLED'", id);
        if (n == 0) {
            Integer exists = jdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE id = ?", Integer.class, id);
            if (exists == null || exists == 0) throw new NoSuchElementException("order " + id);
            return; // already cancelled: idempotent, and no duplicate event
        }
        outbox.append("order", id.toString(), "OrderCancelled", new OrderCancelled(id.toString(), reason));
    }

    public Map<String, Object> get(UUID id) {
        return jdbc.queryForMap("SELECT id, customer_id, status, total_cents FROM orders WHERE id = ?", id);
    }
}
