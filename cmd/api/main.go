package main

import (
	"kafak-bridge/internal/controller"
	"kafak-bridge/internal/database"
	"kafak-bridge/internal/repository"
	config "kafak-bridge/pkg/configs"
	"log"

	"github.com/labstack/echo/v4"
)

func main() {
	cfg := config.Load("./pkg/configs")
	dbRepo, err := database.NewPostgresRepository(cfg.GetDSN())
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	InitRoutes(dbRepo)
}

func InitRoutes(dbRepo repository.EventRepository) {
	e := echo.New()
	eventController := controller.EventsController{EventRepo: dbRepo}
	e.GET("/kafka-bridge/apis/v1/records", eventController.GetEventsListPaginated())

	log.Fatal(e.Start("0.0.0.0:3000"))
}
