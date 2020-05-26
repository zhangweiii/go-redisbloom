package goredisbloom

import (
	"reflect"
	"testing"
)

func TestMain(t *testing.T) {
	key := "testRedisBloomFilter"
	client := NewClient("127.0.0.1:6379", "", 1)
	defer client.redisClient.Del(key)
	client.BFCreate(key, 0.0001, 10000)
	assertInt(t, client.BFAdd(key, "test"), 1)
	assertInt(t, client.BFAdd(key, "test"), 0)
	assertInt(t, client.BFExists(key, "test"), 1)
	assertInt(t, client.BFExists(key, "noexist"), 0)
	assertInts(t, client.BFMAdd(key, "test", "test1", "test2"),
		[]int{0, 1, 1})
	assertInts(t, client.BFMExists(key, "noexist", "test", "test1"),
		[]int{0, 1, 1})
}

func assertInt(t *testing.T, expected int, got int) {
	t.Helper()

	if got != expected {
		t.Errorf("expected %d got %d", expected, got)
	}
}

func assertInts(t *testing.T, expected []int, got []int) {
	t.Helper()

	if reflect.DeepEqual(expected, got) {
		t.Errorf("expected %v got %v", expected, got)
	}
}
