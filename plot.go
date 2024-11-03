package pg_mini

type animatedGraph struct {
	graph   *Graph
	enabled bool
}

func (ag *animatedGraph) Render() {
	if !ag.enabled {
		return
	}

	ag.graph.Print(true)
}
