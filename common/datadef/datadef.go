// Copyright 2018 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package datadef defines some common types.
package datadef

import (
	"io"
	"io/ioutil"

	"github.com/someonegg/queue/common/errdef"
)

type Valider interface {
	Valid() bool
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

type ValidMarshaler interface {
	Valider
	Marshaler
}

type Unmarshaler interface {
	Unmarshal([]byte) error
}

type ValidUnmarshaler interface {
	Valider
	Unmarshaler
}

func ReadUnmarshal(r io.Reader, o Unmarshaler) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	err = o.Unmarshal(b)
	if err != nil {
		return err
	}

	return nil
}

func ReadUnmarshalValid(r io.Reader, o ValidUnmarshaler) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	err = o.Unmarshal(b)
	if err != nil {
		return err
	}

	if !o.Valid() {
		return errdef.ErrParameter
	}

	return nil
}

func MarshalWrite(w io.Writer, o Marshaler) error {
	b, err := o.Marshal()
	if err != nil {
		return err
	}

	_, err = w.Write(b)
	return err
}
