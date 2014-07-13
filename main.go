package main

import (
	//"fmt"
	"strings"
	goredis "github.com/wangchll/redis"
	redis "github.com/dotcloud/go-redis-server"
)

type MyHandler struct {
	redis.DefaultHandler
}

// Get override the DefaultHandler's method.
func (h *MyHandler) Get(combine string) ([]byte, error) {
	keys := strings.Split(combine, "|")
	//fmt.Printf("%q\n", keys)
	//循环调用
	chs := make([]chan string, len(keys))
	for i, key := range keys {
		chs[i] = make(chan string)
		//@todo 暂时写死host
		go getJson("127.0.0.1:6379", key, chs[i])
	}
	//字符串数组
	res := make([]string,len(keys))
	for k, ch := range chs {
		res[k] = <-ch
		//println(<-ch)
	}
	return []byte(strings.Join(res, "%%%")), nil
}

// Test2 implement a new command. Non-redis standard, but it is possible.
// This function needs to be registered.
func Test2(key string) ([]byte, error) {
	var client goredis.Client
	client.Addr = "127.0.0.1:6379"
	var k = key
	val, _ := client.Get(k)
	return val, nil
}

func MGet(key string, keys ...string) ([][]byte, error) {
	keys = append([]string{key}, keys...)
	if len(keys) == 0 {
		return nil, nil
	}
	chs := make([]chan string, len(keys))
	var ret [][]byte
	for i, key := range keys {
		chs[i] = make(chan string, 1024)
		//@todo 暂时写死host
		go getJsonPlus(key, chs[i])
	}
	for _, ch := range chs {
		ret = append(ret, []byte(<-ch))
	}
	return ret, nil
}

func CombineGet(combine string) ([]byte, error) {
	keys := strings.Split(combine, "|")
	//fmt.Printf("%q\n", keys)
	//循环调用
	chs := make([]chan string, len(keys))
	for i, key := range keys {
		chs[i] = make(chan string)
		//@todo 暂时写死host
		go getJson("127.0.0.1:6379", key, chs[i])
	}
	//字符串数组
	res := make([]string,len(keys))
	for k, ch := range chs {
		res[k] = <-ch
		//println(<-ch)
	}
	return []byte(strings.Join(res, "|")), nil
}

func getJson(addr string, key string, ch chan string) {
	var client goredis.Client
	client.Addr = addr
	var k = key
	//client.Set(key, []byte("world"))
	val, _ := client.Get(k)
	ch <- string(val)
}

func getJsonPlus(addrkey string, ch chan string) {
	defer close(ch)
	rs := strings.Split(addrkey, "@")
	var addr, key string
	if len(rs) > 2 {
		ch <- string("")
	} else {
		if len(rs) == 2 {
			addr = rs[0]
			key = rs[1]
		} else {
			addr = "127.0.0.1:6379"
			key = addrkey
		}
		var client goredis.Client
		client.Addr = addr
		var k = key
		//client.Set(key, []byte("world"))
		val, err := client.Get(k)
		if err != nil {
			ch <- string("")
		} else {
			ch <- string(val)
		}
	}
	
}

func main() {
	myhandler := &MyHandler{}
	server, err := redis.NewServer(redis.DefaultConfig().Handler(myhandler))
	if err != nil {
		panic(err)
	}
	if err := server.RegisterFct("test2", Test2); err != nil {
		panic(err)
	}
	if err := server.RegisterFct("combineget", CombineGet); err != nil {
		panic(err)
	}
	if err := server.RegisterFct("mget", MGet); err != nil {
		panic(err)
	}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}