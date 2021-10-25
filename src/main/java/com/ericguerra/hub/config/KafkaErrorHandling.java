package com.ericguerra.hub.config;

import com.ericguerra.hub.events.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Retry in place with exponential backoff (blocking the partition keeps per-order ordering intact),
 * then park the record on "<topic>.DLT" with the exception in headers. Poison messages that can never
 * succeed skip retries entirely.
 */
@Configuration
@Profile("!test")
public class KafkaErrorHandling {

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(template,
                (ConsumerRecord<?, ?> rec, Exception ex) -> new TopicPartition(rec.topic() + ".DLT", rec.partition()));

        ExponentialBackOff backoff = new ExponentialBackOff(500, 2.0);
        backoff.setMaxInterval(30_000);
        backoff.setMaxElapsedTime(5 * 60_000); // ~5 minutes of retries, e.g. a DB failover

        DefaultErrorHandler h = new DefaultErrorHandler(dlt, backoff);
        h.addNotRetryableExceptions(EventEnvelope.MalformedEventException.class, DeserializationException.class,
                IllegalArgumentException.class);
        h.addRetryableExceptions(TransientDataAccessException.class);
        return h;
    }
}
