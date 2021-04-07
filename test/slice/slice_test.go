package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type ListNode struct {
	Value int
	Next *ListNode
}

func TestSliceOperator(t *testing.T) {
	a := []int{1,2,3}
	b := a[:]
	b[0] = 0
	//fmt.Println(a, b)
	if a[0] != 0 {
		t.Fatal("a[0] should be 0")
	}
}

func TestSliceAppend(t *testing.T) {
	a := []*ListNode{&ListNode{
		Value: 1,
	}, &ListNode{
		Value: 2,
	}, &ListNode{
		Value: 3,
	}}

	b := []*ListNode{}
	b = append(b, a...)
	b[0].Value = 0
	assert.Equal(t, 0, a[0].Value, "a[0] should be 0")
}

func TestSliceCopy(t *testing.T) {
	a := []*ListNode{&ListNode{
		Value: 1,
	}, &ListNode{
		Value: 2,
	}, &ListNode{
		Value: 3,
	}}

	m := copy(a, a[1:])
	a[2] = nil
	assert.Equal(t, 2, m)
	assert.Equal(t, 2, a[0].Value)
	assert.Equal(t, 3, a[1].Value)
	assert.Nil(t, a[2])
}

