package go_delay

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

var (
	addr = "127.0.0.1:6379"
	//password = "austin"
)

func TestDelayQueue_consume(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: addr,
		//Password: password,
	})
	redisCli.FlushDB(context.Background())

	// 测试参数
	batchSize := 500
	retryCount := 3
	deliveryCount := make(map[string]int)
	cb := func(s string) bool {
		deliveryCount[s]++
		i, _ := strconv.ParseInt(s, 10, 64)
		return i%2 == 0
	}

	delayQueue := NewQueue("TestDelayQueue_consume", redisCli, cb, UseHashTagKey()).
		WithFetchInterval(time.Millisecond * 50).
		WithMaxConsumeDuration(0).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithFetchLimit(2)

	// 1. 发送延迟消息
	for i := 0; i < batchSize; i++ {
		err := delayQueue.SendDelayMsg(strconv.Itoa(i), 0, WithRetryCount(retryCount))
		if err != nil {
			t.Error(err)
		}
	}

	// 2. 消费消息
	for i := 0; i < 10*batchSize; i++ {
		err := delayQueue.consume()
		if err != nil {
			t.Errorf("消费错误: %v", err)
			return
		}
	}

	// 3. 重试验证
	for k, v := range deliveryCount {
		i, _ := strconv.ParseInt(k, 10, 64)
		if i%2 == 0 {
			if v != 1 {
				t.Errorf("expect 1 delivery, actual %d", v)
			}
		} else {
			if v != retryCount+1 {
				t.Errorf("expect %d delivery, actual %d", retryCount+1, v)
			}
		}
	}
}
