package com.ericguerra.hub.inventory;

import com.ericguerra.hub.events.EventEnvelope;
import com.ericguerra.hub.events.EventEnvelope.MalformedEventException;
import com.ericguerra.hub.inbox.Inbox;
import com.ericguerra.hub.outbox.Outbox;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Reserves stock when an order is placed and releases it on cancel. Each event is handled in one DB
 * transaction together with its inbox claim and any follow-up events (outbox), so a crash at any
 * point either applies everything or nothing.
 */
@Service
public class InventoryHandler {
    public static final String CONSUMER = "inventory-reservations";
    private static final Logger log = LoggerFactory.getLogger(InventoryHandler.class);

    public record Shortage(String orderId, List<Map<String, Object>> missing) {}
    public record Reserved(String orderId) {}

    private final JdbcTemplate jdbc;
    private final Inbox inbox;
    private final Outbox outbox;
    private final ObjectMapper json;
    private final MeterRegistry metrics;

    public InventoryHandler(JdbcTemplate jdbc, Inbox inbox, Outbox outbox, ObjectMapper json, MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.inbox = inbox;
        this.outbox = outbox;
        this.json = json;
        this.metrics = metrics;
    }

    /** @return false if the message was a duplicate. */
    @Transactional
    public boolean handle(EventEnvelope e) {
        if (!inbox.claim(CONSUMER, e.id())) {
            metrics.counter("hub.events.duplicate", "consumer", CONSUMER).increment();
            log.debug("duplicate {} {}", e.type(), e.id());
            return false;
        }
        JsonNode body = parse(e);
        switch (e.type()) {
            case "OrderPlaced" -> reserve(body);
            case "OrderCancelled" -> release(body.path("orderId").asText());
            default -> metrics.counter("hub.events.ignored", "type", e.type()).increment(); // claimed so it isn't re-examined
        }
        metrics.counter("hub.events.processed", "consumer", CONSUMER, "type", e.type()).increment();
        return true;
    }

    private void reserve(JsonNode order) {
        String orderId = order.path("orderId").asText();
        List<Map<String, Object>> missing = new ArrayList<>();
        // Lock rows in a fixed (sku) order so two orders touching the same SKUs can't deadlock.
        List<JsonNode> lines = new ArrayList<>();
        order.path("lines").forEach(lines::add);
        lines.sort((a, b) -> a.path("sku").asText().compareTo(b.path("sku").asText()));

        for (JsonNode l : lines) {
            String sku = l.path("sku").asText();
            int qty = l.path("qty").asInt();
            int updated = jdbc.update("UPDATE inventory SET reserved = reserved + ? WHERE sku = ? AND on_hand - reserved >= ?", qty, sku, qty);
            if (updated == 0) missing.add(Map.of("sku", sku, "requested", qty));
        }
        if (!missing.isEmpty()) {
            // All-or-nothing: throwing rolls back the partial reservations *and* the inbox claim;
            // the listener then records the shortage in a fresh transaction.
            throw new ShortageException(orderId, missing);
        }
        for (JsonNode l : lines) {
            jdbc.update("INSERT INTO reservations (order_id, sku, qty) VALUES (?, ?, ?)", UUID.fromString(orderId), l.path("sku").asText(), l.path("qty").asInt());
        }
        outbox.append("inventory", orderId, "InventoryReserved", new Reserved(orderId));
    }

    private void release(String orderId) {
        List<Map<String, Object>> held = jdbc.queryForList("SELECT sku, qty FROM reservations WHERE order_id = ? ORDER BY sku", UUID.fromString(orderId));
        for (Map<String, Object> r : held) {
            jdbc.update("UPDATE inventory SET reserved = reserved - ? WHERE sku = ?", r.get("qty"), r.get("sku"));
        }
        jdbc.update("DELETE FROM reservations WHERE order_id = ?", UUID.fromString(orderId));
    }

    /** Called after a ShortageException rolled the reservation attempt back. */
    @Transactional
    public void recordShortage(EventEnvelope e, ShortageException s) {
        if (!inbox.claim(CONSUMER, e.id())) return;
        outbox.append("inventory", s.orderId, "InventoryShortage", new Shortage(s.orderId, s.missing));
        metrics.counter("hub.inventory.shortage").increment();
    }

    private JsonNode parse(EventEnvelope e) {
        try {
            return json.readTree(e.payload());
        } catch (Exception ex) {
            throw new MalformedEventException("payload of " + e.id() + " is not JSON", ex);
        }
    }

    public static class ShortageException extends RuntimeException {
        public final String orderId;
        public final List<Map<String, Object>> missing;

        public ShortageException(String orderId, List<Map<String, Object>> missing) {
            super("insufficient stock for order " + orderId + ": " + missing);
            this.orderId = orderId;
            this.missing = missing;
        }
    }
}
