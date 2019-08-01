// Copyright 2018 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package errdef

import "errors"

var (
	ErrUnknown      = errors.New("unknown")
	ErrParameter    = errors.New("wrong parameter")
	ErrCancel       = errors.New("cancel")
	ErrRunOut       = errors.New("run out")
	ErrNotFound     = errors.New("not found")
	ErrAlreadyExist = errors.New("already exist")
)
