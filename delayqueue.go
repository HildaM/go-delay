package go_delay

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"strconv"
	"sync"
	"time"
)

var (
	DEFAULT_MAXCONSUMEDURATION = 5 * time.Second
	DEAFULT_MSGTTL             = time.Hour
	DEFAULT_RETRYCOUNT         = 3
	DEFAULT_FETCHINTERVAL      = time.Second
	DEFAULT_CONCURRENT         = 1
)

type DelayQueue struct {
	name     string
	redisCli RedisCli          // Redis 服务
	cb       func(string) bool // 确认消费成功的回调函数

	// 不同队列的Key
	pendingKey    string // 等待队列	ZSet(message id -> delivery time)
	readyKey      string // 准备队列	List(message id)
	unAckKey      string // 发送队列 ZSet(message id -> retry time)
	retryKey      string // 重试队列 List(message id)
	retryCountKey string // 重试次数记录 Map(message id -> count)
	garbageKey    string // 死信队列 Set(message id)

	useHashTag bool
	ticker     *time.Ticker
	logger     *log.Logger
	close      chan struct{}

	// 延迟队列配置信息
	maxConsumeDuration time.Duration
	msgTTL             time.Duration
	defaultRetryCount  uint
	fetchInterval      time.Duration // 拉去消息的时间间隔
	fetchLimit         uint          // 拉去消息的频次限制
	concurrent         uint          // 并发限制
}

// 自定义redis错误
var NilErr = errors.New("nil")

// RedisCli redis client抽象
type RedisCli interface {
	Eval(script string, keys []string, args []interface{}) (interface{}, error) // args should be string, integer or float
	Set(key string, value string, expiration time.Duration) error
	Get(key string) (string, error)
	Del(keys []string) error
	HSet(key string, field string, value string) error
	HDel(key string, fields []string) error
	SMembers(key string) ([]string, error)
	SRem(key string, members []string) error
	ZAdd(key string, values map[string]float64) error
	ZRem(key string, fields []string) error
}

type hashTagKeyOpt int

// Redis HashTag --- 选择key的部分内容进行hash
func UseHashTagKey() interface{} {
	return hashTagKeyOpt(1)
}

// NewQueue0
func NewQueue0(name string, cli RedisCli, callback func(string) bool, opts ...interface{}) *DelayQueue {
	if name == "" {
		panic("Delayqueue name is empty")
	}
	if cli == nil {
		panic("RedisCli is required")
	}
	if callback == nil {
		panic("callback function is required")
	}

	useHashTag := false
	for _, opt := range opts {
		switch opt.(type) {
		case hashTagKeyOpt:
			useHashTag = true
		}
	}

	var keyPrefix string
	if useHashTag {
		keyPrefix = "{delayqueue:" + name + "}"
	} else {
		keyPrefix = "delayqueue" + name
	}

	return &DelayQueue{
		name:               name,
		redisCli:           cli,
		cb:                 callback,
		pendingKey:         keyPrefix + ":pending",
		readyKey:           keyPrefix + ":ready",
		unAckKey:           keyPrefix + ":unack",
		retryKey:           keyPrefix + ":retry",
		retryCountKey:      keyPrefix + ":retry:count",
		garbageKey:         keyPrefix + ":garbage",
		useHashTag:         useHashTag,
		close:              make(chan struct{}, 1),
		maxConsumeDuration: DEFAULT_MAXCONSUMEDURATION,
		msgTTL:             DEAFULT_MSGTTL,
		defaultRetryCount:  uint(DEFAULT_RETRYCOUNT),
		fetchInterval:      DEFAULT_FETCHINTERVAL,
		concurrent:         uint(DEFAULT_CONCURRENT),
		logger:             log.Default(),
	}
}

// 额外配置函数 ---- 建造者模式
/**
queue := NewQueueOnCluster("test", redisCli, cb).
	WithFetchInterval(time.Millisecond * 50).
	WithMaxConsumeDuration(0).
	WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
	WithFetchLimit(2).
	WithConcurrent(1)
*/
func (q *DelayQueue) WithLogger(logger *log.Logger) *DelayQueue {
	q.logger = logger
	return q
}
func (q *DelayQueue) WithFetchInterval(d time.Duration) *DelayQueue {
	q.fetchInterval = d
	return q
}
func (q *DelayQueue) WithMaxConsumeDuration(d time.Duration) *DelayQueue {
	q.maxConsumeDuration = d
	return q
}
func (q *DelayQueue) WithFetchLimit(limit uint) *DelayQueue {
	q.fetchLimit = limit
	return q
}
func (q *DelayQueue) WithConcurrent(c uint) *DelayQueue {
	if c == 0 {
		return q
	}
	q.concurrent = c
	return q
}
func (q *DelayQueue) WithDefaultRetryCount(count uint) *DelayQueue {
	q.defaultRetryCount = count
	return q
}

type retryCountOpt int

// WithRetryCount set retry count for a msg
// example: queue.SendDelayMsg(payload, duration, delayqueue.WithRetryCount(3))
func WithRetryCount(count int) interface{} {
	return retryCountOpt(count)
}

type msgTTLOpt time.Duration

// WithMsgTTL set ttl for a msg
// example: queue.SendDelayMsg(payload, duration, delayqueue.WithMsgTTL(Hour))
func WithMsgTTL(d time.Duration) interface{} {
	return msgTTLOpt(d)
}

// ############ END ############

func (q *DelayQueue) genMsgKey(idStr string) string {
	if q.useHashTag {
		return "{delayqueue:" + q.name + "}" + ":msg:" + idStr
	}
	return "delayqueue:" + q.name + ":msg:" + idStr
}

func (q *DelayQueue) SendScheduleMsg(payload string, t time.Time, opts ...interface{}) error {
	// 1. 解析参数
	retryCount := q.defaultRetryCount
	for _, opt := range opts {
		switch o := opt.(type) {
		case retryCountOpt:
			retryCount = uint(o)
		case msgTTLOpt:
			q.msgTTL = time.Duration(o)
		}
	}

	// 2. 生成消息id
	idStr := uuid.Must(uuid.NewRandom()).String()
	nowTime := time.Now()

	// 3. 存储消息
	msgTTL := t.Sub(nowTime) + q.msgTTL
	err := q.redisCli.Set(q.genMsgKey(idStr), payload, msgTTL)
	if err != nil {
		return fmt.Errorf("存储原始消息失败: %v", err)
	}
	// 存储重试次数
	err = q.redisCli.HSet(q.retryCountKey, idStr, strconv.Itoa(int(retryCount)))
	if err != nil {
		return fmt.Errorf("存储消息重试次数失败: %v", err)
	}

	// 4. 推送到准备队列
	err = q.redisCli.ZAdd(q.pendingKey, map[string]float64{idStr: float64(t.Unix())})
	if err != nil {
		return fmt.Errorf("推送到准备队列失败: %v", err)
	}

	return nil
}

func (q *DelayQueue) SendDelayMsg(payload string, duration time.Duration, opts ...interface{}) error {
	t := time.Now().Add(duration)
	return q.SendScheduleMsg(payload, t, opts...)
}

// pending2ReadyScript redis lua脚本：从pending到ready队列
// keys: pendingKey, readyKey
// argv: currentTime
const pending2ReadyScript = `
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get ready msg
if (#msgs == 0) then return end

local args2 = {'LPush', KEYS[2]} -- push into ready
for _,v in ipairs(msgs) do
	table.insert(args2, v) 
    if (#args2 == 4000) then
		redis.call(unpack(args2))
		args2 = {'LPush', KEYS[2]}
	end
end
if (#args2 > 2) then 
	redis.call(unpack(args2))
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from pending
`

func (q *DelayQueue) pending2Ready() error {
	nowTime := time.Now().Unix()
	keys := []string{q.pendingKey, q.readyKey}
	_, err := q.redisCli.Eval(pending2ReadyScript, keys, []interface{}{nowTime})
	if err != nil && err != NilErr {
		return fmt.Errorf("执行脚本pending2ReadyScript失败: %v", err)
	}

	return nil
}

// ready2UnackScript atomically moves messages from ready to unack
// keys: readyKey/retryKey, unackKey
// argv: retryTime
const ready2UnackScript = `
local msg = redis.call('RPop', KEYS[1])
if (not msg) then return end
redis.call('ZAdd', KEYS[2], ARGV[1], msg)
return msg
`

func (q *DelayQueue) ready2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.readyKey, q.unAckKey}

	res, err := q.redisCli.Eval(ready2UnackScript, keys, []interface{}{retryTime})
	if err != nil {
		if err == NilErr {
			return "", err
		}
		return "", fmt.Errorf("执行脚本ready2UnackScript错误: %v", err)
	}

	str, ok := res.(string)
	if !ok {
		return "", fmt.Errorf("错误结果: %#v", res)
	}
	return str, nil
}

func (q *DelayQueue) retry2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.retryKey, q.unAckKey}
	ret, err := q.redisCli.Eval(ready2UnackScript, keys, []interface{}{retryTime, q.retryKey, q.unAckKey})
	if err == NilErr {
		return "", NilErr
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	return str, nil
}

// unack2RetryScript atomically moves messages from unack to retry which remaining retry count greater than 0,
// and moves messages from unack to garbage which  retry count is 0
// Because DelayQueue cannot determine garbage message before eval unack2RetryScript, so it cannot pass keys parameter to redisCli.Eval
// Therefore unack2RetryScript moves garbage message to garbageKey instead of deleting directly
// keys: unackKey, retryCountKey, retryKey, garbageKey
// argv: currentTime
const unack2RetryScript = `
local unack2retry = function(msgs)
	local retryCounts = redis.call('HMGet', KEYS[2], unpack(msgs)) -- get retry count
	for i,v in ipairs(retryCounts) do
		local k = msgs[i]
		if v ~= false and v ~= nil and v ~= '' and tonumber(v) > 0 then
			redis.call("HIncrBy", KEYS[2], k, -1) -- reduce retry count
			redis.call("LPush", KEYS[3], k) -- add to retry
		else
			redis.call("HDel", KEYS[2], k) -- del retry count
			redis.call("SAdd", KEYS[4], k) -- add to garbage
		end
	end
end

local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get retry msg
if (#msgs == 0) then return end
if #msgs < 4000 then
	unack2retry(msgs)
else
	local buf = {}
	for _,v in ipairs(msgs) do
		table.insert(buf, v)
		if #buf == 4000 then
			unack2retry(buf)
			buf = {}
		end
	end
	if (#buf > 0) then
		unack2retry(buf)
	end
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from unack
`

func (q *DelayQueue) unack2Retry() error {
	keys := []string{q.unAckKey, q.retryCountKey, q.retryKey, q.garbageKey}
	nowTime := time.Now()

	_, err := q.redisCli.Eval(unack2RetryScript, keys, []interface{}{nowTime})
	if err != nil {
		return fmt.Errorf("unack to retry script failed: %v", err)
	}

	return nil
}

func (q *DelayQueue) garbageCollect() error {
	msgIds, err := q.redisCli.SMembers(q.garbageKey)
	if err != nil {
		return fmt.Errorf("SMember执行失败: %v", err)
	}

	// 获取msg的key
	msgKeys := make([]string, 0, len(msgIds))
	for _, idStr := range msgIds {
		msgKeys = append(msgKeys, q.genMsgKey(idStr))
	}

	// 删除消息
	err = q.redisCli.Del(msgKeys)
	if err != nil && err != NilErr {
		return fmt.Errorf("del命令执行失败: %v", err)
	}
	err = q.redisCli.SRem(q.garbageKey, msgIds)
	if err != nil && err != NilErr {
		return fmt.Errorf("清除garbage队列失败: %v", err)
	}

	return nil
}

func (q *DelayQueue) callback(idStr string) error {
	payload, err := q.redisCli.Get(q.genMsgKey(idStr))
	if err != nil {
		if err == NilErr {
			return err
		}
		return fmt.Errorf("获取消息payload错误: %v", err)
	}

	ack := q.cb(payload)
	if ack {
		err = q.ack(idStr)
	} else {
		err = q.nack(idStr)
	}

	return err
}

func (q *DelayQueue) ack(idStr string) error {
	err := q.redisCli.ZRem(q.unAckKey, []string{idStr})
	if err != nil {
		return fmt.Errorf("unack队列消息移除失败: %v", err)
	}

	// 删除消息
	_ = q.redisCli.Del([]string{q.genMsgKey(idStr)})
	_ = q.redisCli.HDel(q.retryCountKey, []string{idStr})

	return nil
}

func (q *DelayQueue) nack(idStr string) error {
	// 重试消息失败，得更新重试时间
	err := q.redisCli.ZAdd(q.unAckKey, map[string]float64{
		idStr: float64(time.Now().Unix()),
	})
	if err != nil {
		return fmt.Errorf("nack方法执行失败: %v", err)
	}

	return nil
}

// batchCallback 并发callback. 必须等待所有callback方法返回后才能退出，否则实际消费消息数量会超过limit
func (q *DelayQueue) batchCallback(ids []string) {
	if len(ids) == 1 || q.concurrent == 1 {
		for _, id := range ids {
			err := q.callback(id)
			if err != nil {
				q.logger.Printf("消费 %s 失败: %v", id, err)
			}
		}
		return
	}

	// 内存队列
	ch := make(chan string, len(ids))
	for _, id := range ids {
		ch <- id
	}
	close(ch)

	// 并发执行
	wg := sync.WaitGroup{}
	concurrent := int(q.concurrent)
	if concurrent > len(ids) {
		concurrent = len(ids)
	}
	wg.Add(concurrent)

	for i := 0; i < concurrent; i++ {
		go func() {
			defer wg.Done()
			for id := range ch {
				err := q.callback(id)
				if err != nil {
					q.logger.Printf("消费 %s 失败: %v", id, err)
				}
			}
		}()
	}

	wg.Wait()
}

// ######## 消费消息的核心方法 ########
func (q *DelayQueue) consume() error {
	// 1. pending -> ready
	err := q.pending2Ready()
	if err != nil {
		return err
	}

	// 2. 消费
	ids := make([]string, 0, q.fetchLimit)
	for {
		idStr, err := q.ready2Unack()
		if err != nil {
			if err == NilErr { // 完结
				break
			}
			return err
		}

		ids = append(ids, idStr)

		// 超过限流阈值
		if q.fetchLimit > 0 && len(ids) >= int(q.fetchLimit) {
			break
		}
	}
	// 批量发送消息
	if len(ids) > 0 {
		q.batchCallback(ids)
	}

	// 3. 消息重试
	// unack --> ready
	err = q.unack2Retry()
	if err != nil {
		return nil
	}
	// 处理死信消息
	err = q.garbageCollect()
	if err != nil {
		return nil
	}

	// retry
	ids = make([]string, 0, q.fetchLimit)
	for {
		idStr, err := q.retry2Unack()
		if err != nil {
			if err == NilErr {
				break
			}
			return err
		}

		ids = append(ids, idStr)
		if q.fetchLimit > 0 && len(ids) >= int(q.fetchLimit) {
			break
		}
	}
	if len(ids) > 0 {
		q.batchCallback(ids)
	}

	return nil
}

// StartConsume 创建消费队列
func (q *DelayQueue) StartConsume() <-chan struct{} {
	q.ticker = time.NewTicker(q.fetchInterval)
	done := make(chan struct{})
	go func() {
	tickerLoop:
		for {
			select {
			case <-q.ticker.C:
				err := q.consume()
				if err != nil {
					log.Printf("消费错误: %v", err)
				}
			case <-q.close:
				break tickerLoop
			}
		}
		close(done)
	}()

	return done
}

func (q *DelayQueue) StopConsume() {
	close(q.close)
	if q.ticker != nil {
		q.ticker.Stop()
	}
}
