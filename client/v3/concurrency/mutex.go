// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package concurrency

import (
	"context"
	"errors"
	"fmt"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	v3 "go.etcd.io/etcd/client/v3"
)

// ErrLocked is returned by TryLock when Mutex is already locked by another session.
var ErrLocked = errors.New("mutex: Locked by another session")
var ErrSessionExpired = errors.New("mutex: session is expired")

// Mutex implements the sync Locker interface with etcd
type Mutex struct {
	s *Session

	pfx   string // 锁的共同前缀 pfx，如 "/service/lock/"
	myKey string // 当前持有锁的客户端的 leaseid 值（完整 Key 的组成为 pfx+"/"+leaseid）
	myRev int64  // revision，理解为当前持有锁的 Revision（修改数） 编号 或者是 CreateRevision
	hdr   *pb.ResponseHeader
}

// 例如：/test/lock + "/"
func NewMutex(s *Session, pfx string) *Mutex {
	return &Mutex{s, pfx + "/", "", -1, nil}
}

// TryLock locks the mutex if not already locked by another session.
// If lock is held by another session, return immediately after attempting necessary cleanup
// The ctx argument is used for the sending/receiving Txn RPC.
func (m *Mutex) TryLock(ctx context.Context) error {
	resp, err := m.tryAcquire(ctx)
	if err != nil {
		return err
	}
	// 通过对比自身的revision和最先创建的key的revision得出谁获得了锁
	// 例如 自身revision:5,最先创建的key createRevision:3  那么不获得锁,进入waitDeletes
	// 自身revision:5,最先创建的key createRevision:5  那么获得锁
	// 获取当前实际拿到锁的KEY
	// if no key on prefix / the minimum rev is key, already hold the lock
	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	// 比较如果当前没有人获得锁（第一次场景）
	// 或者锁的 owner 的 CreateRevision 等于当前的 key 的 CreateRevision，则表示m.myKey即为拿到锁的key，不用新建，直接使用即可
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == m.myRev {
		m.hdr = resp.Header
		return nil
	}
	client := m.s.Client()
	// Cannot lock, so delete the key
	if _, err := client.Delete(ctx, m.myKey); err != nil {
		return err
	}
	m.myKey = "\x00"
	m.myRev = -1
	return ErrLocked
}

// Lock locks the mutex with a cancelable context. If the context is canceled
// while trying to acquire the lock, the mutex tries to clean its stale lock entry.
func (m *Mutex) Lock(ctx context.Context) error {
	resp, err := m.tryAcquire(ctx)
	if err != nil {
		return err
	}
	// if no key on prefix / the minimum rev is key, already hold the lock
	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == m.myRev {
		m.hdr = resp.Header
		return nil
	}
	client := m.s.Client()
	// 等待其他程序释放锁,并删除其他revisions
	// 走到这里代表没有获得锁，需要等待之前的锁被释放，即 CreateRevision 小于当前 CreateRevision 的 kv 被删除
	// 阻塞等待 Owner 释放锁
	// wait for deletion revisions prior to myKey
	// TODO: early termination if the session key is deleted before other session keys with smaller revisions.
	_, werr := waitDeletes(ctx, client, m.pfx, m.myRev-1)
	// release lock key if wait failed
	if werr != nil {
		m.Unlock(client.Ctx())
		return werr
	}

	// make sure the session is not expired, and the owner key still exists.
	gresp, werr := client.Get(ctx, m.myKey)
	if werr != nil {
		m.Unlock(client.Ctx())
		return werr
	}

	if len(gresp.Kvs) == 0 { // is the session key lost?
		return ErrSessionExpired
	}
	m.hdr = gresp.Header

	return nil
}

func (m *Mutex) tryAcquire(ctx context.Context) (*v3.TxnResponse, error) {
	s := m.s
	client := m.s.Client()
	//下面的 m.pfx 就是 prefix，是传进来的前缀，后面的 s.Lease() 会返回一个租约，是一个 int64 的整数，和 session 有关系
	//mykex 先理解为是 / prefix/leaseid 这样的结构
	m.myKey = fmt.Sprintf("%s%x", m.pfx, s.Lease())
	//使用事务机制
	//这里定义一个 cmp 方法，比较上面 m.myKey 的 CreateRevision 是否为 0，等于 0 表示目前不存在该 key，
	//需要执行 Put 操作，不等于 0 表示对应的 key 已经创建了，需要执行 Get 操作
	cmp := v3.Compare(v3.CreateRevision(m.myKey), "=", 0)
	//则put key,并设置租约
	put := v3.OpPut(m.myKey, "", v3.WithLease(s.Lease()))
	//否则 获取这个key,重用租约中的锁(这里主要目的是在于重入)
	//通过第二次获取锁,判断锁是否存在来支持重入
	//所以只要租约一致,那么是可以重入的.
	get := v3.OpGet(m.myKey)
	//通过前缀获取最先创建的key,获取当前锁的真正持有者
	getOwner := v3.OpGet(m.pfx, v3.WithFirstCreate()...)
	//获取到自身的revision(注意,此处CreateRevision和Revision不一定相等)
	// Txn 事务，判断 cmp 的条件是否成立，成立执行 Then，不成立执行 Else，最终执行 Commit()
	resp, err := client.Txn(ctx).If(cmp).Then(put, getOwner).Else(get, getOwner).Commit()
	if err != nil {
		return nil, err
	}
	m.myRev = resp.Header.Revision
	if !resp.Succeeded {
		m.myRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	return resp, nil
}

func (m *Mutex) Unlock(ctx context.Context) error {
	client := m.s.Client()
	if _, err := client.Delete(ctx, m.myKey); err != nil {
		return err
	}
	m.myKey = "\x00"
	m.myRev = -1
	return nil
}

func (m *Mutex) IsOwner() v3.Cmp {
	return v3.Compare(v3.CreateRevision(m.myKey), "=", m.myRev)
}

func (m *Mutex) Key() string { return m.myKey }

// Header is the response header received from etcd on acquiring the lock.
func (m *Mutex) Header() *pb.ResponseHeader { return m.hdr }

type lockerMutex struct{ *Mutex }

func (lm *lockerMutex) Lock() {
	client := lm.s.Client()
	if err := lm.Mutex.Lock(client.Ctx()); err != nil {
		panic(err)
	}
}
func (lm *lockerMutex) Unlock() {
	client := lm.s.Client()
	if err := lm.Mutex.Unlock(client.Ctx()); err != nil {
		panic(err)
	}
}

// NewLocker creates a sync.Locker backed by an etcd mutex.
func NewLocker(s *Session, pfx string) sync.Locker {
	return &lockerMutex{NewMutex(s, pfx)}
}
