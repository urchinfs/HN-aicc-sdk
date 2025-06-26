package types

import (
	"errors"
)

var (
	ErrorInvalidParameter    = errors.New("invalid parameters")
	ErrorNotExists           = errors.New("not exists")
	ErrorNotAllowed          = errors.New("too much task, wait a moment and try again")
	ErrorInternal            = errors.New("internal error")
	ErrorNotImplement        = errors.New("not implement")
	ErrorJsonMarshalFailed   = errors.New("json marshal failed")
	ErrorJsonUnmarshalFailed = errors.New("json unmarshal failed")
)
