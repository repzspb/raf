package model

// Position указывает, куда брокер записал сообщение.
type Position struct {
	// Topic — имя топика, в котором подтверждена запись сообщения.
	Topic string
	// Partition — партиция, в которую записано сообщение.
	Partition int
	// Offset — подтверждённая позиция сообщения внутри партиции.
	Offset int64
}
