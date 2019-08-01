// Copyright 2018 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package queue manage queue's logic.
package queue

import (
	"expvar"
	"sync"
	"time"

	sl "github.com/someonegg/gocontainer/skiplist"

	"github.com/someonegg/queue/common/datadef"
	"github.com/someonegg/queue/common/errdef"
	"github.com/someonegg/queue/protodef"
)

var (
	evarElementAdd = expvar.NewInt("element_add")
	evarElementDel = expvar.NewInt("element_del")

	evarQueuePush   = expvar.NewInt("queue_push")
	evarQueueRepush = expvar.NewInt("queue_repush")
	evarQueuePop    = expvar.NewInt("queue_pop")
	evarQueueRemove = expvar.NewInt("queue_remove")

	evarQueueCount = expvar.NewInt("queue_count")
)

type Queue interface {
	sync.Locker

	// lockfree
	Name() string
	Len() int

	// ErrRunOut ErrAlreadyExist
	Push(key, value string, priority int64, force bool) (pos protodef.QUEUEPOS, rank int, err error)

	// ErrRunOut ErrAlreadyExist
	Repush(key, value string, pos protodef.QUEUEPOS) (rank int, err error)

	// ErrRunOut
	Pop(numMin, numMax int) (pops []protodef.PopElement, err error)

	// ErrNotFound
	Remove(key string, pos protodef.QUEUEPOS) error

	// concurrently-access
	PublishNotice()
}

type NoticePublisher interface {
	Publish(channel string, message datadef.Marshaler)
}

type element struct {
	key string
	val string
	pos protodef.QUEUEPOS
}

func elementCompare(l, r sl.Scorable) int {
	var lpos, rpos protodef.QUEUEPOS

	switch e := l.(type) {
	case protodef.QUEUEPOS:
		lpos = e
	case *element:
		lpos = e.pos
	default:
		panic(l)
	}

	switch e := r.(type) {
	case protodef.QUEUEPOS:
		rpos = e
	case *element:
		rpos = e.pos
	default:
		panic(r)
	}

	return lpos.Compare(rpos)
}

type queue struct {
	sync.Mutex
	name string
	m    *Manager

	el *sl.List
	em map[string]*sl.Element

	noticeLocker sync.Mutex
	notices      []datadef.Marshaler
}

func newQueue(name string, m *Manager) *queue {
	return &queue{
		name: name,
		m:    m,
		el:   sl.NewList(elementCompare),
		em:   make(map[string]*sl.Element),
	}
}

func (q *queue) Name() string {
	return q.name
}

func (q *queue) Len() int {
	return len(q.em)
}

// rErr : ErrRunOut ErrAlreadyExist(no force)
func (q *queue) Push(key, value string, priority int64, force bool) (pos protodef.QUEUEPOS, rank int, rErr error) {
	if q.Len() > q.m.queueMaxLen {
		rErr = errdef.ErrRunOut
		return
	}

	if se, found := q.em[key]; found {
		if !force {
			rErr = errdef.ErrAlreadyExist
			return
		}

		q.del(key, se)

		q.appendNotice(
			&protodef.NoticeInsertRemove{
				Pos:    se.Value.(*element).pos,
				Number: -1,
				Length: q.Len(),
			})

		evarQueueRemove.Add(1)
	}

	e := &element{
		key: key,
		val: value,
		pos: protodef.QUEUEPOS{
			Priority:  priority,
			TimeStamp: time.Now().UnixNano(),
		},
	}
	pos = e.pos

	rank = q.add(e)

	// if last element(rank+1 == Len), dont publish notice
	if rank+1 < q.Len() {
		q.appendNotice(
			&protodef.NoticeInsertRemove{
				Pos:    e.pos,
				Number: 1,
				Length: q.Len(),
			})
	}

	evarQueuePush.Add(1)
	return
}

// rErr : ErrRunOut ErrAlreadyExist
func (q *queue) Repush(key, value string, pos protodef.QUEUEPOS) (rank int, rErr error) {
	se, found := q.em[key]
	if found {
		e := se.Value.(*element)
		if e.pos.Compare(pos) != 0 {
			rErr = errdef.ErrAlreadyExist
			return
		}
		// return reversed rank
		rank = q.Len() - q.el.Rank(se) - 1
		return
	}

	if q.Len() > q.m.queueMaxLen {
		rErr = errdef.ErrRunOut
		return
	}

	e := &element{
		key: key,
		val: value,
		pos: pos,
	}

	rank = q.add(e)

	// if last element(rank+1 == Len), dont publish notice
	if rank+1 < q.Len() {
		q.appendNotice(
			&protodef.NoticeInsertRemove{
				Pos:    e.pos,
				Number: 1,
				Length: q.Len(),
			})
	}

	evarQueueRepush.Add(1)
	return
}

// rErr : ErrRunOut
func (q *queue) Pop(numMin, numMax int) (pops []protodef.PopElement, rErr error) {
	total := q.Len()
	if total < numMin {
		rErr = errdef.ErrRunOut
		return
	}

	num := numMax
	if num > total {
		num = total
	}

	var last *element

	pops = make([]protodef.PopElement, num)
	for i := 0; i < num; i++ {
		se := q.el.Back()
		e := se.Value.(*element)

		q.del(e.key, se)
		pops[i].Key = e.key
		pops[i].Val = e.val
		pops[i].Pos = e.pos
		last = e
	}

	q.appendNotice(
		&protodef.NoticeInsertRemove{
			Pos:    last.pos,
			Number: -num,
			Length: q.Len(),
		})

	evarQueuePop.Add(int64(num))
	return
}

// rErr : ErrNotFound
func (q *queue) Remove(key string, pos protodef.QUEUEPOS) error {
	se, found := q.em[key]
	if !found {
		return errdef.ErrNotFound
	}

	e := se.Value.(*element)
	if pos.Valid() && e.pos.Compare(pos) != 0 {
		return errdef.ErrNotFound
	}

	q.del(key, se)

	q.appendNotice(
		&protodef.NoticeInsertRemove{
			Pos:    e.pos,
			Number: -1,
			Length: q.Len(),
		})

	evarQueueRemove.Add(1)
	return nil
}

func (q *queue) PublishNotice() {
	notices := q.drainNotice()
	for _, notice := range notices {
		q.m.publisher.Publish(q.name, notice)
	}
}

func (q *queue) add(e *element) int {
	evarElementAdd.Add(1)

	se := q.el.Add(e)
	q.em[e.key] = se

	rank := q.el.Rank(se)
	// return reversed rank
	return q.Len() - rank - 1
}

func (q *queue) del(key string, se *sl.Element) {
	evarElementDel.Add(1)

	q.el.Remove(se)
	delete(q.em, key)
}

func (q *queue) appendNotice(n datadef.Marshaler) {
	evarNoticeGenerate.Add(1)

	q.noticeLocker.Lock()
	defer q.noticeLocker.Unlock()
	q.notices = append(q.notices, n)
}

func (q *queue) drainNotice() []datadef.Marshaler {
	q.noticeLocker.Lock()
	defer q.noticeLocker.Unlock()
	notices := q.notices
	q.notices = nil
	return notices
}

// a sync wrapper
type syncQueue struct {
	*queue
}

func (q syncQueue) Push(key, value string, priority int64, force bool) (protodef.QUEUEPOS, int, error) {
	defer q.PublishNotice()

	q.Lock()
	defer q.Unlock()
	return q.queue.Push(key, value, priority, force)
}

func (q syncQueue) Repush(key, value string, pos protodef.QUEUEPOS) (int, error) {
	defer q.PublishNotice()

	q.Lock()
	defer q.Unlock()
	return q.queue.Repush(key, value, pos)
}

func (q syncQueue) Pop(numMin, numMax int) ([]protodef.PopElement, error) {
	defer q.PublishNotice()

	q.Lock()
	defer q.Unlock()
	return q.queue.Pop(numMin, numMax)
}

func (q syncQueue) Remove(key string, pos protodef.QUEUEPOS) error {
	defer q.PublishNotice()

	q.Lock()
	defer q.Unlock()
	return q.queue.Remove(key, pos)
}

type Manager struct {
	queueMaxLen int
	publisher   NoticePublisher

	locker sync.RWMutex
	queues map[string]*queue
}

func NewManager(queueMaxLen int, publisher NoticePublisher) *Manager {
	return &Manager{
		queueMaxLen: queueMaxLen,
		publisher:   publisher,
		queues:      make(map[string]*queue),
	}
}

func (m *Manager) getQueue(name string) *queue {
	m.locker.RLock()
	defer m.locker.RUnlock()
	return m.queues[name]
}

func (m *Manager) queue(name string) *queue {
	q := m.getQueue(name)
	if q != nil {
		return q
	}

	m.locker.Lock()
	defer m.locker.Unlock()
	q = newQueue(name, m)
	m.queues[name] = q

	evarQueueCount.Add(1)
	return q
}

// manually lock (by call Lock)
// manually publish notice (by call PublishNotice)
func (m *Manager) GetRawQueue(name string) Queue {
	q := m.getQueue(name)
	if q == nil {
		return nil
	}
	return q
}

// manually lock (by call Lock)
// manually publish notice (by call PublishNotice)
// create if not exist.
func (m *Manager) RawQueue(name string) Queue {
	return m.queue(name)
}

// automatically lock
// automatically publish notice
func (m *Manager) GetSyncQueue(name string) Queue {
	q := m.getQueue(name)
	if q == nil {
		return nil
	}
	return syncQueue{q}
}

// automatically lock
// automatically publish notice
// create if not exist.
func (m *Manager) SyncQueue(name string) Queue {
	return syncQueue{m.queue(name)}
}
