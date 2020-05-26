package goredisbloom

import (
	"github.com/go-redis/redis"
	"github.com/vmihailenco/msgpack"
)

// Client redis bloom client
type Client struct {
	redisClient *redis.Client
}

// RedisParamsArray .
type RedisParamsArray []string

// MarshalBinary implement encoding.BinaryMarshaler
func (s RedisParamsArray) MarshalBinary() ([]byte, error) {
	return msgpack.Marshal(s)
}

// UnmarshalBinary implement encoding.BinaryUnmarshaler
func (s RedisParamsArray) UnmarshalBinary(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

// NewClient new client
func NewClient(addr, passwd string, db int) *Client {
	c := &Client{}
	c.redisClient = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: passwd,
		DB:       db,
	})

	return c
}

// SetClient set redis client with redis client
func (c *Client) SetClient(client *redis.Client) {
	c.redisClient = client
}

// BFCreate Creates an empty Bloom Filter
// with a given desired error ratio and initial capacity.
func (c *Client) BFCreate(key string, errorRate float64, capacity int) string {
	result, _ := c.redisClient.Do("BF.RESERVE", key, errorRate, capacity).Result()
	if result == nil {
		return ""
	}
	return result.(string)
}

// BFAdd Adds an item to the Bloom Filter,
// creating the filter if it does not yet exist.
func (c *Client) BFAdd(key, item string) int {
	result, _ := c.redisClient.Do("BF.ADD", key, item).Result()
	return convertToInt(result)
}

// BFMAdd Adds one or more items to the Bloom Filter,
// creating the filter if it does not yet exist.
// This command operates identically to BF.ADD except it allows multiple inputs
// and returns multiple values.
func (c *Client) BFMAdd(key string, items ...string) []int {
	arr := make(RedisParamsArray, len(items)+1)
	arr = append(arr, key)
	for _, item := range items {
		arr = append(arr, item)
	}
	result, _ := c.redisClient.Do("BF.MADD", key, arr).Result()

	return convertToInts(result)
}

// BFExists Determines whether an item may exist in the Bloom Filter or not.
func (c *Client) BFExists(key, item string) int {
	result, _ := c.redisClient.Do("BF.EXISTS", key, item).Result()
	return convertToInt(result)
}

// BFMExists Determines if one or more items may exist in the filter or not.
func (c *Client) BFMExists(key string, items ...string) []int {
	arr := make(RedisParamsArray, len(items)+1)
	arr = append(arr, key)
	for _, item := range items {
		arr = append(arr, item)
	}
	result, _ := c.redisClient.Do("BF.MEXISTS", key, arr).Result()
	return convertToInts(result)
}

func convertToInt(v interface{}) int {
	if v == nil {
		return 0
	}
	return int(v.(int64))
}

func convertToInts(v interface{}) []int {
	var result []int
	for _, vv := range v.([]interface{}) {
		result = append(result, convertToInt(vv))
	}
	return result
}
