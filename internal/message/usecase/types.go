package usecase

func (s *Service) ListTypes() []string {
	return s.codec.MessageTypes()
}
