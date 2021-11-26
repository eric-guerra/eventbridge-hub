package com.ericguerra.hub.inventory;

import com.ericguerra.hub.events.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class OrderEventsListener {
    private final InventoryHandler handler;

    public OrderEventsListener(InventoryHandler handler) {
        this.handler = handler;
    }

    @KafkaListener(topics = "order.events", groupId = InventoryHandler.CONSUMER, concurrency = "3")
    public void onOrderEvent(ConsumerRecord<String, String> rec) {
        process(handler, EventEnvelope.from(rec));
    }

    /** Shared by the Kafka listener and tests: a shortage is a business outcome, not an error. */
    static void process(InventoryHandler handler, EventEnvelope e) {
        try {
            handler.handle(e);
        } catch (InventoryHandler.ShortageException s) {
            handler.recordShortage(e, s);
        }
    }
}
