// Copyright 2018 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package queue

import (
	"context"
	"expvar"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/someonegg/gox/syncx"

	"github.com/someonegg/queue/common/datadef"
)

var (
	evarMqConnBreak = expvar.NewInt("mqconn_break")

	evarNoticeGenerate = expvar.NewInt("notice_generate")
	evarNoticeDiscard  = expvar.NewInt("notice_discard")
	evarNoticePublish  = expvar.NewInt("notice_publish")
)

type RedisPublisherConfig struct {
	Addr string
	Pass string

	BufferSize int
	WorkerNum  int
}

func NewRedisPublisher(conf *RedisPublisherConfig) NoticePublisher {
	ctx, quitF := context.WithCancel(context.Background())

	t := &noticePublisher{
		quitF:    quitF,
		stopD:    syncx.NewDoneChan(),
		server:   conf.Addr,
		password: conf.Pass,
		noticeC:  make(chan noticeItem, conf.BufferSize),
	}

	for i := 0; i < conf.WorkerNum; i++ {
		t.workWG.Add(1)
		go t.work(ctx)
	}

	// monitor work
	go func() {
		t.workWG.Wait()
		t.stopD.SetDone()
	}()

	return t
}

type noticeItem struct {
	channel string
	message datadef.Marshaler
}

type noticePublisher struct {
	quitF  context.CancelFunc
	workWG sync.WaitGroup
	stopD  syncx.DoneChan

	server   string
	password string

	noticeC chan noticeItem
}

func (p *noticePublisher) Close() error {
	p.quitF()
	return nil
}

func (p *noticePublisher) Publish(channel string, message datadef.Marshaler) {
	n := noticeItem{channel, message}
	select {
	case <-p.stopD:
	case p.noticeC <- n:
	}
}

func (p *noticePublisher) work(ctx context.Context) {
	var conn redis.Conn

	defer func() {
		p.ending(conn)
	}()

	const BUFSIZE = 50
	var buf [BUFSIZE]noticeItem

	const BUFTIME = 100 * time.Millisecond
	bT := time.After(BUFTIME)

	needMaint := false
	for idx, q := 0, false; !q; {
		select {
		case <-ctx.Done():
			q = true
		case buf[idx] = <-p.noticeC:
			idx++
			if idx == BUFSIZE {
				conn = p.publish(conn, buf[0:idx], needMaint)
				idx = 0
				needMaint = false
			}
		case <-bT:
			bT = time.After(BUFTIME)
			if idx > 0 {
				conn = p.publish(conn, buf[0:idx], needMaint)
				idx = 0
				needMaint = false
			} else {
				needMaint = true
			}
		}
	}
}

func (p *noticePublisher) ending(conn redis.Conn) {
	recover()

	p.workWG.Done()

	if conn != nil {
		conn.Close()
		conn = nil
	}
}

func (p *noticePublisher) publish(conn redis.Conn, items []noticeItem, needMaint bool) redis.Conn {
	if needMaint {
		p.maintConn(conn)
	}

	conn = p.prepare(conn)
	if conn == nil {
		evarNoticeDiscard.Add(int64(len(items)))
		return nil
	}

	for _, item := range items {
		md, err := item.message.Marshal()
		if err != nil {
			panic(err)
		}
		conn.Send("PUBLISH", item.channel, md)
	}

	conn.Flush()
	conn.Do("")

	evarNoticePublish.Add(int64(len(items)))
	return conn
}

func (p *noticePublisher) prepare(conn redis.Conn) redis.Conn {
	if conn != nil && conn.Err() != nil {
		conn.Close()
		conn = nil

		evarMqConnBreak.Add(1)
	}
	if conn != nil {
		return conn
	}

	conn, err := redis.Dial("tcp", p.server, redis.DialPassword(p.password))
	if err != nil {
		return nil
	}

	return conn
}

func (p *noticePublisher) maintConn(conn redis.Conn) {
	if conn != nil {
		conn.Do("PING")
	}
}
