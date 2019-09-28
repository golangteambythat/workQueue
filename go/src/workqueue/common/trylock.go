/*
 * Created: 2019-09-05 Thursday 09:56:54
 * Author : 郑东哲
 * Email : i_zhengdongzhe@cvte.com
 * -----
 * Last Modified: 2019-09-06 Friday 19:55:51
 * Modified By: 郑东哲
 * -----
 * Description: trylock
 */

package common

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	mutexLocked = 1 << iota
)

// Mutex 锁
type Mutex struct {
	in sync.Mutex
}

// Lock 获取锁
func (m *Mutex) Lock() {
	m.in.Lock()
}

// Unlock 释放锁
func (m *Mutex) Unlock() {
	m.in.Unlock()
}

// TryLock 尝试获取锁
func (m *Mutex) TryLock() bool {
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.in)), 0, mutexLocked)
}

// IsLocked 锁是否已被获取
func (m *Mutex) IsLocked() bool {
	return atomic.LoadInt32((*int32)(unsafe.Pointer(&m.in))) != 0
}
