package usecase

import "context"

// Example подготавливает JSON по явно указанному контракту.
func (s *Service) Example(
	ctx context.Context,
	name string,
) ([]byte, error) {
	if err := s.codec.ValidateType(name); err != nil {
		return nil, &TypeError{
			Name: name,
			Err:  err,
		}
	}
	value, err := s.codec.Example(ctx, name)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &ValidationError{Err: err}
	}
	return value, nil
}
