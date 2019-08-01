// Copyright 2018 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package protodef

import (
	"bytes"
	"fmt"
)

// queue position
type QUEUEPOS struct {
	Priority  int64
	TimeStamp int64
}

func (v QUEUEPOS) Valid() bool {
	return v.TimeStamp > 0
}

func (v QUEUEPOS) String() string {
	return fmt.Sprintf("%d.%d", v.Priority, v.TimeStamp)
}

func (v *QUEUEPOS) Set(s string) {
	fmt.Sscanf(s, "%d.%d", &v.Priority, &v.TimeStamp)
}

func (v QUEUEPOS) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, `"%d.%d"`, v.Priority, v.TimeStamp)
	return buf.Bytes(), nil
}

func (v *QUEUEPOS) UnmarshalJSON(b []byte) error {
	buf := bytes.NewBuffer(b)
	_, err := fmt.Fscanf(buf, `"%d.%d"`, &v.Priority, &v.TimeStamp)
	return err
}

// return
//
//	<0 if v <  a
//	 0 if v == a
//	>0 if v >  a
func (v QUEUEPOS) Compare(a QUEUEPOS) int {
	if v.Priority < a.Priority {
		return -1
	} else if v.Priority > a.Priority {
		return 1
	}

	if v.TimeStamp > a.TimeStamp {
		return -1
	} else if v.TimeStamp < a.TimeStamp {
		return 1
	}

	return 0
}

type PopElement struct {
	Key string   `json:"key"`
	Val string   `json:"val"`
	Pos QUEUEPOS `json:"pos"`
}
