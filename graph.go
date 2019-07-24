package libdag

import (
	"container/list"
	"fmt"
)

type DagGraphNode struct {
	Name     string
	Indegree int        //入度
	Outputs  *list.List //输出节点
}

//邻接表
type DagGraph struct {
	Nodes map[string]*DagGraphNode
}

func NewGraph() *DagGraph {
	return &DagGraph{
		Nodes: make(map[string]*DagGraphNode, 0),
	}
}

func ParseGraph(dagConf *DagConfig) (*DagGraph, error) {
	output2node := make(map[string]string)
	var err error = nil

	for _, node := range dagConf.Nodes {
		for outputName, _ := range node.Output {
			// 防止参数名重复
			if outputNode, ok := output2node[outputName]; ok {
				err = fmt.Errorf("duplicate output arg name in (%s,%s)", outputNode, node.Name)
				break
			} else {
				output2node[outputName] = node.Name
			}
		}
	}

	if (err != nil) {
		return nil, err
	}

	graph := NewGraph()

	//按照input 和 output之间关键设置图的拓扑
	var parseDependency = func(node *NodeConfig) {
		for _, inputName := range node.Input {
			if outputNode, ok := output2node[inputName]; ok {
				graph.AddEdge(outputNode, node.Name)
			}
		}
	}

	for _, node := range dagConf.Nodes {
		graph.AddNode(node.Name)
	}

	for _, node := range dagConf.Nodes {
		parseDependency(node)
	}

	return graph, nil
}

//增加节点
func (this *DagGraph) AddNode(name string) {
	node := &DagGraphNode{
		Name:     name,
		Indegree: 0,
		Outputs:  new(list.List),
	}
	node.Outputs.Init()
	this.Nodes[name] = node
}

//建立边
func (this *DagGraph) AddEdge(from string, to string) {
	toNode := this.Nodes[to]
	fromNode := this.Nodes[from]

	toNode.Indegree += 1
	fromNode.Outputs.PushBack(to)
}

//拓扑排序
func (this *DagGraph) Toposort() ([]*DagGraphNode, error) {
	roots := new(list.List)
	roots.Init()

	for _, node := range this.Nodes {
		if (node.Indegree == 0) {
			roots.PushBack(node)
		}
	}

	result := make([]*DagGraphNode, 0)

	for (roots.Len() > 0) {
		r := this.Nodes[roots.Remove(roots.Front()).(*DagGraphNode).Name]
		result = append(result, r)
		for it := r.Outputs.Front(); it != nil; it = it.Next() {
			edge := it.Value.(string)
			edgeNode := this.Nodes[edge]
			edgeNode.Indegree -= 1

			if (edgeNode.Indegree == 0) {
				roots.PushBack(edgeNode)
			}
		}
	}

	if (len(result) != len(this.Nodes)) {
		return nil, fmt.Errorf("found circular dependencies in dag,detail:[%s]", this.Nodes)
	}

	return result, nil
}
