package closer

import (
	"context"
	"fmt"
	"sync"
)

// Dependency presents a service resource, could be released.
// User out of this package should be never able to release Dependency manually.
type Dependency struct {
	releaser Releaser
}

const (
	Ready status = iota
	Released
)

// Closer is a closer pattern for graceful shutdown.
// Closer walks on dependencies and emit release of each resource.
type Closer struct {
	Status status
	mu     sync.Mutex
	g      graph
}

type (
	Releaser func(context.Context) error
	graph    map[*Dependency]map[*Dependency]bool
	status   int
)

// Add instantiate a new dependant resource with releaser and dependencies.
func (c *Closer) Add(r Releaser, ds ...*Dependency) (*Dependency, error) {
	const op = "adding dependency"
	c.mu.Lock()
	defer c.mu.Unlock()

	// Verify dependencies.
	for _, d := range ds {
		if d == nil {
			return nil, fmt.Errorf("%s: dependency is nil", op)
		}
		if _, ok := c.g[d]; !ok {
			return nil, fmt.Errorf("%s: dependency not associated with current canceler", op)
		}
	}

	if c.Status != Ready {
		return nil, fmt.Errorf("%s: canceller actually not ready", op)
	}
	if c.g == nil {
		c.g = make(graph, len(ds))
	}
	from := &Dependency{releaser: r}
	c.g[from] = make(map[*Dependency]bool)
	for _, to := range ds {
		c.g[from][to] = true
	}

	return from, nil
}

func (c *Closer) Close(ctx context.Context) (<-chan error, error) {
	var (
		errC = make(chan error)
	)
	go func() {
		defer close(errC)
		for {
			layerC, ok := c.g.Layer(ctx)
			if !ok {
				break
			}
			c.g.Release(ctx, layerC, errC)
		}
	}()
	return errC, ctx.Err()
}

func (g graph) Layer(ctx context.Context) (<-chan *Dependency, bool) {
	deps, ok := g.topologicalLayer()
	if !ok {
		return nil, false
	}
	ch := make(chan *Dependency, len(deps))
	go func() {
		defer close(ch)
		for _, dep := range deps {
			select {
			case ch <- dep:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, true
}

func (g graph) Release(ctx context.Context, deps <-chan *Dependency, errC chan<- error) {
	for dep := range deps {
		err := dep.releaser(ctx)
		if err == nil {
			continue
		}
		select {
		case errC <- err:
		case <-ctx.Done():
			return
		}
	}
}

func (g graph) topologicalLayer() (top []*Dependency, ok bool) {
	if len(g) == 0 {
		return nil, false
	}
	visited := make(map[*Dependency]bool)
	for from, ends := range g {
		for to := range ends {
			if !g[from][to] {
				continue
			}
			visited[to] = true
		}
	}
	for n := range g {
		if visited[n] {
			continue
		}
		top = append(top, n)
		delete(g, n)
	}
	return top, true
}
