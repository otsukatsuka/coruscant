package errors

type Error interface {
	error
}

type cError struct {
	err     error
	message string
}

func (e *cError) Error() string { return e.err.Error() }

func Wrap(err error, msg ...string) Error {
	var m string
	if len(msg) != 0 {
		m = msg[0]
	}
	e := create(m)
	e.err = err
	return e
}

func NewBufferError(msg string) Error {
	return create(msg)
}

func create(msg string) *cError {
	var e cError
	e.message = msg
	return &e
}
