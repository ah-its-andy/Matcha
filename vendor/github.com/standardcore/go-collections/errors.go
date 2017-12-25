package collections

import "errors"

func ERRORNilReference() error {
	return errors.New("object reference not set to an instance of an object")
}

func ERRORSourceIsNil() error {
	return errors.New("source is nil")
}

func ERRORSourceSequenceEmpty() error {
	return errors.New("the source sequence is empty")
}

func ERRORInputSequenceContainsMoreThanOneElement() error {
	return errors.New("the input sequence contains more than one element")
}
