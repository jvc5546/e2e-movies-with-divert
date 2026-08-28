package com.okteto.rent.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Column;
import jakarta.persistence.Transient;

@Entity
@Table(name = "rentals")
public class Rental {

    @Id
    @Column(name = "id", nullable = false, unique = true)
    private String id;

    @Column(name = "price", nullable = false)
    private String price;

    // Namespace of the worker that processed this rental — written by the Go
    // worker (raw SQL, see rentals/worker/pkg/database/database.go), read here
    // so GET /rent can surface it in the UI as a "processed by" badge.
    @Column(name = "namespace")
    private String namespace;

    // Only used to carry the movie title from the incoming POST /rent body
    // through to the Kafka message header (see RentController) for log
    // correlation — never persisted, and never read back via GET /rent.
    @Transient
    private String title;

    public Rental() {
    }

    public Rental(String id, String price) {
        this.id = id;
        this.price = price;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
