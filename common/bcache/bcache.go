package bcache

/**
 * 内存淘汰算法
 * 基于LRU
 * 为避免不必要的interface转换，目前只支持key与value都为string的情况。
 * By timmu
 * example:
 * cache := common.NewBcache("test_cache",20).Ttl(120*time.Second).Loaderfunc(loader)
 * cache.Get("test1")
 * cache.Set("test2","10000")
 * cache.Expire("test2",120*time.Second)
 * cache.Del("test2")
 */
import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	//"fmt"
	//"encoding/json"
)

var BcacheKeyNotFound = errors.New("Key not found")

type Bcache struct {
	name   string                   //缓存名
	size   int                      //缓存key成员数量
	data   map[string]*list.Element //元数据
	expire *time.Duration           //过期设置
	mu     sync.RWMutex             //读写锁
	load   BcacheLoaderFunc         //自动化载入函数
	mem    int                      //内存占用空间
	items  *list.List               //成员访问排序排序链表
	hit    int32                    //命中缓存
	miss   int32                    //未命中缓存
}

type BcacheLoaderFunc func(string) (string, error)

func NewBcache(name string, lenght int) *Bcache {
	c := &Bcache{
		name:  name,
		size:  lenght,
		data:  make(map[string]*list.Element, lenght),
		items: list.New(),
	}
	//go c.reportStat() // 配合log.ied.com使用，默认注释掉
	return c
}

/**
 * 当key不存在时，调用此方法获取key值，并加入缓存
 */
func (this *Bcache) LoaderFunc(loader BcacheLoaderFunc) *Bcache {
	this.load = func(key string) (string, error) {
		v, err := loader(key)
		return v, err
	}
	return this
}

/**
 * 为每一个key设置默认过期时间
 */
func (this *Bcache) Ttl(ttl time.Duration) *Bcache {
	this.expire = &ttl
	return this
}

/**
 * 设置缓存
 */
func (this *Bcache) Set(key string, value string) bool {
	this.mu.Lock()

	mem := len([]byte(value))
	//check existing
	if item, ok := this.data[key]; ok {
		this.items.MoveToFront(item)
		it := item.Value.(*cItem)
		it.value = value
		this.mem = this.mem - it.mem + mem
		it.mem = mem
	} else {
		if this.items.Len() >= this.size {
			this.delLast(1)
		}
		it := &cItem{
			key:   key,
			value: value,
			mem:   mem,
		}
		if this.expire != nil {
			t := time.Now().Add(*this.expire)
			it.expire = &t
		}

		this.data[key] = this.items.PushFront(it)

		this.mem = this.mem + mem
	}

	this.mu.Unlock()
	return true
}

/**
 * 获取指定key的value
 */
func (this *Bcache) Get(key string) (string, error) {
	this.mu.Lock()
	item, ok := this.data[key]
	if ok {
		it := item.Value.(*cItem)
		if !it.IsExpired() {
			this.items.MoveToFront(item)
			//增加统计 -- 命中
			atomic.AddInt32(&this.hit, 1)
			v := it.value
			this.mu.Unlock()
			return v, nil
		}
		this.removeItem(item)
	}
	this.mu.Unlock()
	//增加统计 -- 未命中
	atomic.AddInt32(&this.miss, 1)
	if this.load != nil {
		v, err := this.load(key)
		if err == nil {
			this.Set(key, v)
			return v, nil
		}
		return "", err
	}
	return "", BcacheKeyNotFound
}

/**
 * 返回所有的key信息
 */
func (this *Bcache) Keys() []string {
	var member []string
	this.mu.RLock()
	for key, _ := range this.data {
		member = append(member, key)
	}
	this.mu.RUnlock()
	return member
}

/**
 * 删除指定缓存
 */
func (this *Bcache) Del(key string) bool {
	return this.del(key)
}

func (this *Bcache) del(key string) bool {
	this.mu.Lock()
	if item, ok := this.data[key]; ok {
		this.removeItem(item)
	}
	this.mu.Unlock()
	return true
}

/**
 * 底层自动上报性能数据
 * 上报命中情况，以及成员个数，内存使用率
 * 配合log.ied.com使用，默认注释掉
 */
// func (this *Bcache) reportStat() {
// 	for range time.Tick(60 * time.Second) {
// 		var report = make(map[string]interface{})
// 		this.mu.RLock()
// 		report["hit"] = atomic.SwapInt32(&this.hit, 0)
// 		report["miss"] = atomic.SwapInt32(&this.miss, 0)
// 		report["name"] = this.name
// 		report["mem"] = this.mem
// 		report["len"] = len(this.data)
// 		databyte, err := json.Marshal(report)
// 		if err == nil {
// 		}
// 		this.mu.RUnlock()
// 	}
// }

/**
 * 返回状态
 */
func (this *Bcache) Stat() map[string]interface{} {
	this.mu.RLock()
	var stat = map[string]interface{}{
		"mem":  this.mem,
		"size": len(this.data),
	}
	this.mu.RUnlock()
	return stat
}

func (this *Bcache) delLast(cnt int) {
	for i := 0; i < cnt; i++ {
		item := this.items.Back()
		if item == nil {
			return
		} else {
			this.removeItem(item)
		}
	}
}

func (this *Bcache) removeItem(e *list.Element) {
	this.items.Remove(e)
	detail := e.Value.(*cItem)
	this.mem = this.mem - detail.mem
	delete(this.data, detail.key)
}

/**
 * 注意 所有的过期设置都是以当前时间向后推移，并非在原有过期时间上去做增加操作
 */
func (this *Bcache) Expire(key string, expiration time.Duration) bool {
	this.mu.Lock()
	if item, ok := this.data[key]; ok {
		it := item.Value.(*cItem)
		t := time.Now().Add(expiration)
		it.expire = &t
	}
	this.mu.Unlock()
	return true
}

type cItem struct {
	expire *time.Time
	key    string
	value  string
	mem    int
}

func (it *cItem) IsExpired() bool {
	if it.expire == nil {
		return false
	}

	return it.expire.Before(time.Now())
}
