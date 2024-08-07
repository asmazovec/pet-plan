package closer_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"plans/cmd/internal/closer"
	"slices"
	"testing"
)

func TestAdd_Single_ShouldRegister(t *testing.T) {
	c := &closer.Closer{}

	res, err := c.Add(nil)

	assert.NotNilf(t, res, "Should register resource.")
	assert.Nil(t, err)
}

func TestAdd_Dependency_ShouldRegister(t *testing.T) {
	c := &closer.Closer{}
	r, _ := c.Add(nil)

	res, err := c.Add(nil, r)

	assert.NotNilf(t, res, "Should register resource")
	assert.Nil(t, err)
}

func TestAdd_MultipleDeps_ShouldRegisterAll(t *testing.T) {
	c := &closer.Closer{}
	r1, _ := c.Add(nil)
	r2, _ := c.Add(nil, r1)
	res, err := c.Add(nil, r1, r2)

	assert.NotNilf(t, res, "Should register resource")
	assert.Nil(t, err)
}

func TestAdd_SameMultipleTimes_ShouldRegisterOnce(t *testing.T) {
	c := &closer.Closer{}
	r1, _ := c.Add(nil)

	res, err := c.Add(nil, r1, r1, r1)

	assert.NotNilf(t, res, "Should register resource")
	assert.Nil(t, err)
}

func TestAdd_NilDependency_ShouldError(t *testing.T) {
	c := &closer.Closer{}

	res, err := c.Add(nil, nil, nil)

	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func TestAdd_NotAssociatedDeps_ShouldError(t *testing.T) {
	c1 := &closer.Closer{}
	c2 := &closer.Closer{}
	r, _ := c1.Add(nil)

	res, err := c2.Add(nil, r)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

type ResourceMock struct {
	Order []int
}

func (r *ResourceMock) OrderedCall(order int, err error) closer.Releaser {
	return func(ctx context.Context) error {
		r.Order = append(r.Order, order)
		return err
	}
}

func TestCancel_Graph_ShouldCancelInOrder(t *testing.T) {
	var (
		r = new(ResourceMock)
		c = new(closer.Closer)
	)

	r1, _ := c.Add(r.OrderedCall(1, nil))
	r2, _ := c.Add(r.OrderedCall(2, nil), r1)
	_, _ = c.Add(r.OrderedCall(3, nil), r2, r1)
	_, _ = c.Add(r.OrderedCall(3, nil), r2)
	errs, _ := c.Close(context.Background())
	for range errs {
	}

	assert.True(t, slices.Equal(r.Order, []int{3, 3, 2, 1}))
}

func TestCancel_GraphWithErrorCall_ShouldAddToErrorsMsg(t *testing.T) {
	var (
		r = new(ResourceMock)
		c = new(closer.Closer)
	)

	errExpected := fmt.Errorf("error")
	r1, _ := c.Add(r.OrderedCall(1, nil))
	r2, _ := c.Add(r.OrderedCall(2, nil), r1)
	_, _ = c.Add(r.OrderedCall(3, errExpected), r2)
	errs, _ := c.Close(context.Background())

	for err := range errs {
		assert.Error(t, err, errExpected)
	}
}
