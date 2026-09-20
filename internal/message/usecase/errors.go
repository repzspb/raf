package usecase

// ValidationError означает, что входные данные не позволяют выполнить сценарий.
type ValidationError struct {
	// Err — причина, по которой входные данные не прошли проверку.
	Err error
}

func (e *ValidationError) Error() string { return e.Err.Error() }
func (e *ValidationError) Unwrap() error { return e.Err }

// TypeError сообщает об отсутствующем или неизвестном типе контракта.
type TypeError struct {
	// Name — запрошенный тип; пустое значение означает, что тип не задан.
	Name string
	// Err — причина ошибки выбора или поиска типа.
	Err error
}

func (e *TypeError) Error() string { return e.Err.Error() }
func (e *TypeError) Unwrap() error { return e.Err }

// TimeoutError позволяет адаптерам сообщать о таймауте без типов библиотеки брокера.
type TimeoutError struct {
	// Err — исходная ошибка таймаута, доступная через errors.Is и errors.As.
	Err error
}

func (e *TimeoutError) Error() string { return e.Err.Error() }
func (e *TimeoutError) Unwrap() error { return e.Err }
func (e *TimeoutError) Timeout() bool { return true }
