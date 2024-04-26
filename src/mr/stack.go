package mr

import (
	"fmt"
	"time"
)

type StackError struct {
	When time.Time
	What string
}

func (e *StackError) Error() string {
	return fmt.Sprintf("at %v, %s", e.When, e.What)
}

type Stack[T any] struct {
	//lock   sync.Mutex // you don't have to do this if you don't want thread safety
	values []T
}

func NewStack[T any]() *Stack[T] {
	return &Stack[T]{make([]T, 0)}
}

func (s *Stack[T]) Push(val T) {
	s.values = append(s.values, val)
}

func (s *Stack[T]) Pop() (error, T) {
	if len(s.values) == 0 {
		var zeroValue T
		err := &StackError{
			When: time.Now(),
			What: "Empty stack",
		}
		return err, zeroValue
	}
	val := s.values[len(s.values)-1]
	s.values = s.values[:len(s.values)-1]
	return nil, val
}

func (s *Stack[T]) List() {
	for i := 0; i < len(s.values); i++ {
		fmt.Println(s.values[i])
	}
}
