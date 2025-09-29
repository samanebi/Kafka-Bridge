package controller

import (
	"kafak-bridge/internal/repository"
	"net/http"

	"github.com/labstack/echo/v4"
)

type EventsController struct {
	EventRepo repository.EventRepository
}

func (e *EventsController) GetEventsListPaginated() func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		var request EventsListRequest
		err := ctx.Bind(&request)
		if err != nil {
			return ctx.JSON(http.StatusBadRequest, echo.Map{"error": "bad request"})
		}

		response, err := e.EventRepo.List(ctx.Request().Context(), request.Page, request.Size)
		if err != nil {
			return ctx.JSON(http.StatusInternalServerError, echo.Map{"error": "internal server error"})
		}

		return ctx.JSON(http.StatusOK, response)
	}
}

type EventsListRequest struct {
	Page int `query:"page"`
	Size int `query:"size"`
}
