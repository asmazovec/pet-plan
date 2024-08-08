package closer_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"plans/cmd/internal/closer"
	"slices"
	"sync"
	"testing"
	"time"
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
	mu    sync.Mutex
	Order []int
}

func (r *ResourceMock) CallOrdered(order int, err error) closer.Releaser {
	return func(ctx context.Context) error {
		r.mu.Lock()
		r.Order = append(r.Order, order)
		r.mu.Unlock()
		return err
	}
}

func (r *ResourceMock) CallOrderedWithTimeout(timeout time.Duration, order int, err error) closer.Releaser {
	ctx, closeCtx := context.WithTimeout(context.Background(), timeout)
	return func(baseCtx context.Context) error {
		defer closeCtx()
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.Order = append(r.Order, order)
			r.mu.Unlock()
		case <-baseCtx.Done():
		}
		return err
	}
}

func TestCancel_ShouldAwaitReleases(t *testing.T) {
	var (
		r = new(ResourceMock)
		c = new(closer.Closer)
	)

	r1, _ := c.Add(r.CallOrderedWithTimeout(100*time.Millisecond, 1, nil))
	r2, _ := c.Add(r.CallOrderedWithTimeout(10*time.Millisecond, 2, nil), r1)
	_, _ = c.Add(r.CallOrderedWithTimeout(80*time.Millisecond, 3, nil), r2, r1)
	_, _ = c.Add(r.CallOrderedWithTimeout(30*time.Millisecond, 4, nil), r2)
	errs := c.Close(context.Background())
	for range errs {
	}

	assert.True(t, slices.Equal(r.Order, []int{4, 3, 2, 1}))
}

func TestCancel_NilReleaser_ShouldNotPanic(t *testing.T) {
	var c = new(closer.Closer)

	_, _ = c.Add(nil)
	errs := c.Close(context.Background())

	<-errs
}

func TestCancel_ShouldReleaseEverySubgraph(t *testing.T) {
	var (
		r = new(ResourceMock)
		c = new(closer.Closer)
	)

	// Subgraph 1
	g1r1, _ := c.Add(r.CallOrdered(3, nil))
	g1r2, _ := c.Add(r.CallOrdered(2, nil), g1r1)
	_, _ = c.Add(r.CallOrdered(1, nil), g1r2, g1r1)

	// Subgraph 2
	g2r1, _ := c.Add(r.CallOrdered(2, nil))
	_, _ = c.Add(r.CallOrdered(1, nil), g2r1)
	_, _ = c.Add(r.CallOrdered(1, nil), g2r1)

	// Subgraph 3
	_, _ = c.Add(r.CallOrdered(1, nil))

	errs := c.Close(context.Background())
	for range errs {
	}

	assert.Equal(t, 7, len(r.Order))
	assert.True(t, slices.Equal(r.Order, []int{1, 1, 1, 1, 2, 2, 3}))
}

func TestCancel_LayerShouldReleaseInParallel(t *testing.T) {
	var (
		r = new(ResourceMock)
		c = new(closer.Closer)
	)
	ctx, done := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer done()

	r1, _ := c.Add(r.CallOrdered(1, nil))
	_, _ = c.Add(r.CallOrderedWithTimeout(100*time.Millisecond, 2, nil), r1)
	_, _ = c.Add(r.CallOrderedWithTimeout(100*time.Millisecond, 2, nil), r1)
	_, _ = c.Add(r.CallOrderedWithTimeout(100*time.Millisecond, 2, nil), r1)
	errs := c.Close(ctx)
	for range errs {
	}

	assert.Nil(t, ctx.Err())
	assert.True(t, slices.Equal(r.Order, []int{2, 2, 2, 1}))
}

func TestCancel_ExpireContext_ShouldStop(t *testing.T) {
	var (
		r = new(ResourceMock)
		c = new(closer.Closer)
	)
	ctx, cancel := context.WithCancel(context.Background())
	r1, _ := c.Add(r.CallOrderedWithTimeout(10*time.Millisecond, 1, nil))
	_, _ = c.Add(r.CallOrdered(2, nil), r1)

	errs := c.Close(ctx)
	_ = <-errs
	cancel()
	_ = <-errs

	assert.Error(t, ctx.Err())
	assert.Equal(t, 1, len(r.Order))
	assert.True(t, slices.Equal(r.Order, []int{2}))
}

func TestCancel_Graph_ShouldCancelInOrder(t *testing.T) {
	var (
		r = new(ResourceMock)
		c = new(closer.Closer)
	)

	r1, _ := c.Add(r.CallOrdered(1, nil))
	r2, _ := c.Add(r.CallOrdered(2, nil), r1)
	_, _ = c.Add(r.CallOrdered(3, nil), r2, r1)
	_, _ = c.Add(r.CallOrdered(3, nil), r2)
	errs := c.Close(context.Background())
	for err := range errs {
		assert.Nil(t, err)
	}

	assert.True(t, slices.Equal(r.Order, []int{3, 3, 2, 1}))
}

func TestCancel_GraphWithErrorCall_ShouldAddToErrorsMsg(t *testing.T) {
	var (
		r = new(ResourceMock)
		c = new(closer.Closer)
	)

	errExpected := fmt.Errorf("error")
	r1, _ := c.Add(r.CallOrdered(1, errExpected))
	r2, _ := c.Add(r.CallOrdered(2, errExpected), r1)
	_, _ = c.Add(r.CallOrdered(3, nil), r2)
	_, _ = c.Add(r.CallOrdered(3, errExpected), r2)
	errs := c.Close(context.Background())

	errCount := 0
	for err := range errs {
		if err != nil {
			errCount++
			assert.Error(t, err, errExpected)
		}
	}
	assert.Equal(t, errCount, 3)
}
