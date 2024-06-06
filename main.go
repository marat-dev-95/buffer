package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-playground/validator/v10"
)

var (
	brokers = "kafka:9092"
	version = sarama.DefaultVersion.String()
	group   = "mygroup"
	topics  = "kek"
)

// приходящие сообщения в наш API
type Message struct {
	PeriodStart         string `json:"period_start" validate:"required"`
	PeriodEnd           string `json:"period_end" validate:"required"`
	PeriodKey           string `json:"period_key" validate:"required"`
	IndicatorToMoID     int    `json:"indicator_to_mo_id" validate:"required"`
	IndicatorToMoFactID int    `json:"indicator_to_mo_fact_id"`
	Value               int    `json:"value" validate:"required"`
	FactTime            string `json:"fact_time" validate:"required"`
	IsPlan              int    `json:"is_plan"`
	AuthUserID          int    `json:"auth_user_id" validate:"required"`
	Comment             string `json:"comment"`
}

func main() {
	log.Println("Starting a new Sarama consumer")

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	//указываем что мы будем помечать успешно отправленные сообщения, чтобы обновлялось смещение и не было дублировании
	config.Producer.Return.Successes = true

	wg := &sync.WaitGroup{}
	wg.Add(2)

	// Создаем одного kafka producer для записи сообщении
	producer := startProducerWithRetry(config)
	defer producer.Close()
	// Запускаем сервер который принимает запросы и записывает в kafka
	go startHTTPServer(producer, wg)
	// Запускаем consumer который получает сообщения из kafka, затем отправляет по API
	go startConsumer(config, wg)

	wg.Wait()
}

func startProducerWithRetry(config *sarama.Config) sarama.SyncProducer {
	var producer sarama.SyncProducer
	var err error
	for {
		producer, err = sarama.NewSyncProducer(strings.Split(brokers, ","), config)
		if err == nil {
			break
		}
		log.Printf("Error creating sync producer: %v. Retrying in 5 seconds...\n", err)
		time.Sleep(5 * time.Second)
	}
	return producer
}

func startHTTPServer(producer sarama.SyncProducer, wg *sync.WaitGroup) {
	defer wg.Done()

	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Logger)

	r.Post("/facts", func(w http.ResponseWriter, r *http.Request) {
		// Разбор данных формы
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			http.Error(w, "Unable to parse form", http.StatusBadRequest)
			return
		}

		// Извлечение значений
		var message Message
		message.PeriodStart = r.FormValue("period_start")
		message.PeriodEnd = r.FormValue("period_end")
		message.PeriodKey = r.FormValue("period_key")
		message.FactTime = r.FormValue("fact_time")
		message.Comment = r.FormValue("comment")

		// Преобразование строковых значений в int
		var err error
		message.IndicatorToMoID, err = strconv.Atoi(r.FormValue("indicator_to_mo_id"))
		if err != nil {
			http.Error(w, "Invalid indicator_to_mo_id", http.StatusBadRequest)
			return
		}

		message.IndicatorToMoFactID, err = strconv.Atoi(r.FormValue("indicator_to_mo_fact_id"))
		if err != nil {
			http.Error(w, "Invalid indicator_to_mo_fact_id", http.StatusBadRequest)
			return
		}

		message.Value, err = strconv.Atoi(r.FormValue("value"))
		if err != nil {
			http.Error(w, "Invalid value", http.StatusBadRequest)
			return
		}

		message.IsPlan, err = strconv.Atoi(r.FormValue("is_plan"))
		if err != nil {
			http.Error(w, "Invalid is_plan", http.StatusBadRequest)
			return
		}

		message.AuthUserID, err = strconv.Atoi(r.FormValue("auth_user_id"))
		if err != nil {
			http.Error(w, "Invalid auth_user_id", http.StatusBadRequest)
			return
		}

		// Валидация запроса
		validate := validator.New()
		if err := validate.Struct(message); err != nil {
			http.Error(w, fmt.Sprintf("Validation error: %v", err), http.StatusBadRequest)
			return
		}

		// сериализуем в json и сохраняем в kafka
		err = produceMessage(producer, message)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error producing message: %v", err), http.StatusInternalServerError)
			return
		}
		response := map[string]string{"status": "ok"}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	log.Println("Starting HTTP server on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}

func produceMessage(producer sarama.SyncProducer, message Message) error {
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topics,
		Value: sarama.ByteEncoder(messageBytes),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Printf("Error producing message: %v\n", err)
		return err
	}
	return nil
}

func startConsumer(config *sarama.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		consumer := Consumer{}
		if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			log.Panicf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

type Consumer struct{}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			// Декодируем сообщение из JSON
			var data Message
			if err := json.Unmarshal(message.Value, &data); err != nil {
				log.Printf("Error decoding message: %v\n", err)
				continue
			}
			// Формируем данные для отправки в формате form-data
			formData := url.Values{}
			formData.Set("period_start", data.PeriodStart)
			formData.Set("period_end", data.PeriodEnd)
			formData.Set("period_key", data.PeriodKey)
			formData.Set("indicator_to_mo_id", strconv.Itoa(data.IndicatorToMoID))
			formData.Set("indicator_to_mo_fact_id", strconv.Itoa(data.IndicatorToMoFactID))
			formData.Set("value", strconv.Itoa(data.Value))
			formData.Set("fact_time", data.FactTime)
			formData.Set("is_plan", strconv.Itoa(data.IsPlan))
			formData.Set("auth_user_id", strconv.Itoa(data.AuthUserID))
			formData.Set("comment", data.Comment)

			req, err := http.NewRequest("POST", "https://development.kpi-drive.ru/_api/facts/save_fact", strings.NewReader(formData.Encode()))
			if err != nil {
				log.Printf("Error creating request: %v\n", err)
				continue
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Authorization", "Bearer 48ab34464a5573519725deb5865cc74c")

			// Отправляем запрос
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Error sending request: %v\n", err)
				continue
			}
			defer resp.Body.Close()

			responseBody, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Error reading response body: %v\n", err)
				return nil
			}
			var responseMap map[string]interface{}
			if err := json.Unmarshal(responseBody, &responseMap); err != nil {
				log.Printf("Error unmarshaling response body: %v\n", err)
				return nil
			}
			// помечаем сообщение только в успешном отправлении, иначе не убираем из очереди
			if responseMap["STATUS"] == "OK" {
				log.Println("sent")
				session.MarkMessage(message, "")
			}

		case <-session.Context().Done():
			return nil
		}
	}
}
