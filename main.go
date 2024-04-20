package main

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	"os"
	"os/signal"
	"producer/model"
)

var topic = "myTopic"
var broker *kafka.Producer

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	broker = p

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	opts := []bot.Option{
		bot.WithDefaultHandler(handler),
	}

	b, err := bot.New("BOT_TOKEN", opts...) // Замените YOUR_BOT_TOKEN на ваш токен бота
	if err != nil {
		panic(err)
	}

	b.Start(ctx)

	// Ожидание завершения работы программы
	<-ctx.Done()
}

func handler(ctx context.Context, b *bot.Bot, update *models.Update) {
	newUser := &model.User{
		Id:          update.Message.Chat.ID,
		LastMessage: update.Message.Text,
	}

	userBytes, err := SerializeObjectToBytes(newUser)
	if err != nil {
		panic(err)
	}

	broker.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(userBytes),
	}, nil)

	b.SendMessage(ctx, &bot.SendMessageParams{
		ChatID: update.Message.Chat.ID,
		Text:   update.Message.Text,
	})
}

func SerializeObjectToBytes(obj interface{}) ([]byte, error) {
	// Сериализуем объект в JSON
	bytes, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
