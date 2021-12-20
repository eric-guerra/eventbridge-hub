package com.ericguerra.hub.inventory;

import com.ericguerra.hub.events.EventEnvelope;
import com.ericguerra.hub.orders.OrderService;
import com.ericguerra.hub.outbox.Outbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.IllegalTransactionStateException;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Plays Debezium's role by reading outbox rows and turning them into the records the router would
 * emit, then feeds them to the consumer, including duplicate deliveries.
 */
@SpringBootTest
@ActiveProfiles("test")
class OutboxInboxFlowTest {
    @Autowired OrderService orders;
    @Autowired InventoryHandler handler;
    @Autowired Outbox outbox;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void stock() {
        jdbc.update("DELETE FROM reservations");
        jdbc.update("DELETE FROM inbox");
        jdbc.update("DELETE FROM outbox");
        jdbc.update("DELETE FROM inventory");
        jdbc.update("INSERT INTO inventory (sku, on_hand, reserved) VALUES ('TEE-M', 10, 0), ('MUG', 2, 0)");
    }

    /** What the EventRouter SMT would publish for each outbox row, oldest first. */
    List<EventEnvelope> drainOutbox(String aggregateType) {
        return jdbc.query("SELECT id, aggregate_id, event_type, payload FROM outbox WHERE aggregate_type = ? ORDER BY created_at",
                (rs, i) -> {
                    ConsumerRecord<String, String> rec = new ConsumerRecord<>(aggregateType + ".events", 0, i, rs.getString("aggregate_id"), rs.getString("payload"));
                    rec.headers().add(new RecordHeader("id", ("\"" + rs.getString("id") + "\"").getBytes(StandardCharsets.UTF_8)));
                    rec.headers().add(new RecordHeader("eventType", rs.getString("event_type").getBytes(StandardCharsets.UTF_8)));
                    return EventEnvelope.from(rec);
                }, aggregateType);
    }

    int reserved(String sku) {
        return jdbc.queryForObject("SELECT reserved FROM inventory WHERE sku = ?", Integer.class, sku);
    }

    @Test
    void placingAnOrderWritesExactlyOneOutboxEventInTheSameTransaction() {
        UUID id = orders.place("c-1", 4200, List.of(new OrderService.Line("TEE-M", 2)));
        List<EventEnvelope> events = drainOutbox("order");
        assertThat(events).hasSize(1);
        assertThat(events.get(0).type()).isEqualTo("OrderPlaced");
        assertThat(events.get(0).aggregateId()).isEqualTo(id.toString());
        assertThat(events.get(0).payload()).contains("\"sku\":\"TEE-M\"");
    }

    @Test
    void failedBusinessTransactionLeavesNoEvent() {
        assertThatThrownBy(() -> orders.place("c-1", 0, List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThat(drainOutbox("order")).isEmpty();
    }

    @Test
    void outboxRefusesToRunOutsideATransaction() {
        assertThatThrownBy(() -> outbox.append("order", "x", "Oops", Map.of()))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void reservationIsAppliedOnceDespiteRedelivery() {
        orders.place("c-1", 4200, List.of(new OrderService.Line("TEE-M", 3)));
        EventEnvelope placed = drainOutbox("order").get(0);

        OrderEventsListener.process(handler, placed);
        OrderEventsListener.process(handler, placed); // consumer crashed before committing the offset
        OrderEventsListener.process(handler, placed);

        assertThat(reserved("TEE-M")).isEqualTo(3);
        assertThat(drainOutbox("inventory")).extracting(EventEnvelope::type).containsExactly("InventoryReserved");
    }

    @Test
    void shortageIsAllOrNothingAndEmitsAnEvent() {
        orders.place("c-2", 9900, List.of(new OrderService.Line("TEE-M", 1), new OrderService.Line("MUG", 5)));
        EventEnvelope placed = drainOutbox("order").get(0);
        OrderEventsListener.process(handler, placed);
        OrderEventsListener.process(handler, placed);

        assertThat(reserved("TEE-M")).as("partial reservation rolled back").isZero();
        assertThat(reserved("MUG")).isZero();
        List<EventEnvelope> inv = drainOutbox("inventory");
        assertThat(inv).extracting(EventEnvelope::type).containsExactly("InventoryShortage");
        assertThat(inv.get(0).payload()).contains("MUG");
    }

    @Test
    void cancelReleasesStockAndIsIdempotent() {
        UUID id = orders.place("c-3", 1000, List.of(new OrderService.Line("TEE-M", 4)));
        OrderEventsListener.process(handler, drainOutbox("order").get(0));
        assertThat(reserved("TEE-M")).isEqualTo(4);

        orders.cancel(id, "customer request");
        orders.cancel(id, "double click");
        List<EventEnvelope> events = drainOutbox("order");
        assertThat(events).extracting(EventEnvelope::type).containsExactly("OrderPlaced", "OrderCancelled");

        OrderEventsListener.process(handler, events.get(1));
        OrderEventsListener.process(handler, events.get(1));
        assertThat(reserved("TEE-M")).isZero();
    }

    @Test
    void malformedRecordsAreRejectedAsNonRetryable() {
        ConsumerRecord<String, String> noHeaders = new ConsumerRecord<>("order.events", 0, 0, "k", "{}");
        assertThatThrownBy(() -> EventEnvelope.from(noHeaders)).isInstanceOf(EventEnvelope.MalformedEventException.class);

        ConsumerRecord<String, String> badJson = new ConsumerRecord<>("order.events", 0, 1, "k", "not json");
        badJson.headers().add(new RecordHeader("id", "m-1".getBytes(StandardCharsets.UTF_8)));
        badJson.headers().add(new RecordHeader("eventType", "OrderPlaced".getBytes(StandardCharsets.UTF_8)));
        assertThatThrownBy(() -> handler.handle(EventEnvelope.from(badJson))).isInstanceOf(EventEnvelope.MalformedEventException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM inbox", Integer.class)).as("claim rolled back").isZero();
    }
}
