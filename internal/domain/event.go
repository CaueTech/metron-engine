package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// 1. Tipo discreto para as categorias
type EventType string

const (
	EventTypeUnknown EventType = iota
	EventTypeOverheat
	EventTypeMechanicalWarning
	EventTypeVoltageDrop
	maxEventType
)

// 2. A Entidade de Domínio
type Event struct {
	id        uuid.UUID  // ID único deste evento
	sourceID  uuid.UUID  // ID da origem (ex: sensor, máquina ou host)
	eventType EventType  // Categoria discreta
	timestamp time.Time  // Momento exato em que o evento ocorreu/foi criado
}

// Getters: mantêm os campos imutáveis de fora do pacote
func (e *Event) getID() uuid.UUID        { return e.id }
func (e *Event) getSourceID() uuid.UUID  { return e.sourceID }
func (e *Event) getType() EventType      { return e.eventType }
func (e *Event) getTimestamp() time.Time { return e.timestamp }