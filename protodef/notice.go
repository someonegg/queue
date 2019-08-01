// Copyright 2018 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package protodef

import "encoding/json"

const (
	NoticeTypeInsertRemove int = 1
)

type NoticeBase struct {
	Type int `json:"typ"`
}

func (n *NoticeBase) Unmarshal(b []byte) error {
	return json.Unmarshal(b, n)
}

// Notify the number of elements insert to (remove from) the queue at the pos.
//
//	insert : number < 0
//	remove : number > 0
type NoticeInsertRemove struct {
	Pos    QUEUEPOS `json:"pos"`
	Number int      `json:"num"`
	Length int      `json:"len"`
}

type noticeInsertRemove struct {
	NoticeBase
	*NoticeInsertRemove
}

func (n *NoticeInsertRemove) Marshal() ([]byte, error) {
	return json.Marshal(
		&noticeInsertRemove{
			NoticeBase: NoticeBase{
				Type: NoticeTypeInsertRemove,
			},
			NoticeInsertRemove: n,
		})
}

func (n *NoticeInsertRemove) Unmarshal(b []byte) error {
	return json.Unmarshal(b, n)
}
