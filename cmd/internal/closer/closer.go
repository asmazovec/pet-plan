package closer

import (
	"context"
	"fmt"
	"sync"
)

type (
	Releaser func(context.Context) error
	graph    map[*Dependency]map[*Dependency]bool
)

// Dependency presents a service resource, could be released.
// User out of this package should be never able to release Dependency manually.
type Dependency struct {
	releaser Releaser
}

// Closer is a closer pattern for graceful shutdown.
// Closer walks on dependencies and emit release of each resource.
type Closer struct {
	mu sync.Mutex
	g  graph
}

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

// Close releases dependencies keeping dependency order.
// If releasers done with errors, they send it to the error channel.
func (c *Closer) Close(ctx context.Context) <-chan error {
	var errC = make(chan error)
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		defer close(errC)
		for {
			layerC, size := c.g.Layer(ctx)
			if size == 0 {
				break
			}
			for err := range c.g.Release(ctx, size, layerC) {
				errC <- err
			}
		}
	}()
	return errC
}

// Release gets a layer pipeline and release it up.
// Results sends to the error channel.
func (g graph) Release(ctx context.Context, workers int, deps <-chan *Dependency) <-chan error {
	var wg sync.WaitGroup
	errC := make(chan error)

	release := func() {
		defer wg.Done()
		for dep := range deps {
			if dep.releaser == nil {
				continue
			}
			select {
			case errC <- dep.releaser(ctx):
			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go release()
	}
	go func() {
		defer close(errC)
		wg.Wait()
	}()

	return errC
}

// Layer produces dependencies from topological layer and sends it to dependency channel.
func (g graph) Layer(ctx context.Context) (<-chan *Dependency, int) {
	deps, ok := g.topologicalLayer()
	if !ok {
		return nil, 0
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
	return ch, len(deps)
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
