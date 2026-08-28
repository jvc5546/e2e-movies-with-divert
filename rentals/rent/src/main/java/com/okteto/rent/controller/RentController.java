package com.okteto.rent.controller;

import com.okteto.rent.model.Rental;
import com.okteto.rent.repository.RentalRepository;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.UUID;

@RestController
public class RentController {
    private static final String KAFKA_TOPIC_RENTALS = "rentals";
    private static final String KAFKA_TOPIC_RETURNS = "returns";

    private final Logger logger = LoggerFactory.getLogger(RentController.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private RentalRepository rentalRepository;

    // This rent-api only ever serves its own personal frontend (it's never
    // diverted/shared), so when a caller sends no baggage header — e.g. real
    // browser traffic, since nothing upstream of a personal deployment sets
    // one — it's always correct to self-tag Kafka messages with our own
    // namespace rather than leave them untagged.
    @Value("${KUBERNETES_NAMESPACE:}")
    private String ownNamespace;

    private String effectiveBaggage(String baggage) {
        if (baggage != null && !baggage.isEmpty()) {
            return baggage;
        }
        return "okteto-divert=" + ownNamespace;
    }

    // Short correlation ID generated once per request. Every worker consuming
    // the shared Kafka topic logs this same ID — whether it accepts or skips
    // the message — so log lines from different namespaces can be matched up
    // as "the same event" instead of relying on timestamps.
    private String newRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    // Title carries no routing meaning — it's attached purely so every log
    // line (accept or skip, on any worker) is human-readable instead of just
    // a bare movie ID.
    private void tagCommonHeaders(ProducerRecord<String, String> record, String baggage, String title, String requestId) {
        record.headers().add(new RecordHeader("baggage", effectiveBaggage(baggage).getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("title", (title != null ? title : "").getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("request-id", requestId.getBytes(StandardCharsets.UTF_8)));
    }

    @GetMapping(path= "/rent/healthz", produces = "application/json")
    Map<String, String> healthz() {
            return Collections.singletonMap("status", "ok");
    }

    @GetMapping(path= "/rent", produces = "application/json")
    List<Rental> getAllRentals() {
        logger.info("Fetching all rentals from database");
        List<Rental> allRentals = rentalRepository.findAll();

        // uncomment to increase price
        //for (Rental allRental : allRentals) {
        //    allRental.setPrice("68.99");
        //} 
        return allRentals;
    }
    
    @PostMapping(path= "/rent", consumes = "application/json", produces = "application/json")
    List<String> rent(@RequestBody Rental rentInput,
                      @RequestHeader(value = "baggage", required = false) String baggage) {
        String movieID = rentInput.getId();
        String price = rentInput.getPrice();
        String title = rentInput.getTitle();
        String requestId = newRequestId();

        logger.info("[req={}] Rent [{},{}] '{}' received", requestId, movieID, price, title);

        // Create ProducerRecord to add custom headers
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC_RENTALS, movieID, price.toString());
        tagCommonHeaders(record, baggage, title, requestId);

        kafkaTemplate.send(record)
        .thenAccept(result -> logger.info("[req={}] Message [{}] delivered with offset {}",
                        requestId,
                        movieID,
                        result.getRecordMetadata().offset()))
        .exceptionally(ex -> {
            logger.warn("[req={}] Unable to deliver message [{}]. {}", requestId, movieID, ex.getMessage());
            return null;
        });


        return new LinkedList<>();
    }

    @PostMapping(path= "/rent/return", consumes = "application/json", produces = "application/json")
    public Map<String, String> returnMovie(@RequestBody ReturnRequest returnRequest,
                                           @RequestHeader(value = "baggage", required = false) String baggage) {
        String movieID = returnRequest.getMovieID();
        String title = returnRequest.getTitle();
        String requestId = newRequestId();

        logger.info("[req={}] Return [{}] '{}' received", requestId, movieID, title);

        // Create ProducerRecord to add custom headers
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC_RETURNS, movieID, movieID);
        tagCommonHeaders(record, baggage, title, requestId);

        kafkaTemplate.send(record)
        .thenAccept(result -> logger.info("[req={}] Return message [{}] delivered with offset {}",
                        requestId,
                        movieID,
                        result.getRecordMetadata().offset()))
        .exceptionally(ex -> {
            logger.warn("[req={}] Unable to deliver return message [{}]. {}", requestId, movieID, ex.getMessage());
            return null;
        });

        return Collections.singletonMap("status", "return processed");
    }

    public static class ReturnRequest {
        @JsonProperty("id")
        private String movieID;

        @JsonProperty("title")
        private String title;

        public void setMovieID(String movieID) {
            this.movieID = movieID;
        }

        public String getMovieID() {
            return movieID;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }
    }
}
