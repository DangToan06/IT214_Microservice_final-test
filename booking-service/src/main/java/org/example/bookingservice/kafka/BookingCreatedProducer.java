package org.example.bookingservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StringUtils;

@Component
public class BookingCreatedProducer {

    private static final Logger log = LoggerFactory.getLogger(BookingCreatedProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public BookingCreatedProducer(KafkaTemplate<String, String> kafkaTemplate,
                                  @Value("${booking.kafka.booking-created-topic:booking-created}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publishBookingCreatedEvent(String customerEmail) {
        if (!StringUtils.hasText(customerEmail)) {
            log.warn("Không gửi sự kiện booking vì email rỗng");
            return;
        }

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    log.info("Gửi sự kiện booking-created cho email={}", customerEmail);
                    kafkaTemplate.send(topic, customerEmail);
                }
            });
        } else {
            log.info("Không có transaction active, gửi sự kiện ngay cho email={}", customerEmail);
            kafkaTemplate.send(topic, customerEmail);
        }
    }
}

