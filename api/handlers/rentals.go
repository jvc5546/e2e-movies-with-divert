package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/okteto/movies/middleware"
)

type Rental struct {
	ID        string `json:"id,omitempty"`
	Price     string `json:"price,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

type Catalog struct {
	ID            int     `json:"id,omitempty"`
	VoteAverage   float64 `json:"vote_average,omitempty"`
	OriginalTitle string  `json:"original_title,omitempty"`
	BackdropPath  string  `json:"backdrop_path,omitempty"`
	Price         float64 `json:"price,omitempty"`
	Overview      string  `json:"overview,omitempty"`
	// Namespace is only set for rentals — it's the personal namespace whose
	// worker actually processed this rental, surfaced in the UI as a "processed
	// by" badge to make cross-namespace Kafka routing visible without a CLI.
	Namespace string `json:"namespace,omitempty"`
}

func GetRentalsWithCatalogInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	catalog, err := getCatalog(ctx)
	if err != nil {
		log.Println(err)
		w.WriteHeader(500)
		return
	}

	if len(catalog) == 0 {
		log.Println("catalog is empty")
		w.WriteHeader(500)
		return
	}

	rentals, err := getRentals(ctx)
	if err != nil {
		log.Println(err)
		w.WriteHeader(500)
		return
	}

	result := []Catalog{}
	for _, r := range rentals {
		for _, m := range catalog {
			if r.ID == strconv.Itoa(m.ID) {
				price, _ := strconv.ParseFloat(r.Price, 64)
				m.Price = price
				m.Namespace = r.Namespace
				result = append(result, m)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func getRentals(ctx context.Context) ([]Rental, error) {

	resp, err := getService(ctx, "http://rent:8080/rent")
	if err != nil {
		return nil, err
	}

	rentals := []Rental{}
	if err := json.Unmarshal(resp, &rentals); err != nil {
		return nil, fmt.Errorf("error unmarshalling rentals: %v", err)
	}

	return rentals, nil
}

func getCatalog(ctx context.Context) ([]Catalog, error) {

	resp, err := getService(ctx, "http://catalog:8080/catalog")
	if err != nil {
		return nil, err
	}

	catalog := []Catalog{}
	if err := json.Unmarshal(resp, &catalog); err != nil {
		return nil, fmt.Errorf("error unmarshalling catalog: %v", err)
	}

	return catalog, nil
}

func getService(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http client")
	}

	// Propagate baggage header
	if baggage := middleware.GetBaggageFromContext(ctx); baggage != "" {
		req.Header.Set("Baggage", baggage)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error calling service: %v", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("error reading the request body: %v", err)
	}

	return body, nil
}
