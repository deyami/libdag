package libdag

import (
	"context"
	"errors"
	"fmt"
	"github.com/deyami/libdag/utils/logs"
	"sync"
)

type JobContext struct {
	sync.RWMutex
	Config          *DagConfig
	GlobalVals      *sync.Map
	HandlerRegistry HandlerManager
}

func NewJobContext(config *DagConfig, registry HandlerManager) *JobContext {
	return &JobContext{Config: config, HandlerRegistry: registry, GlobalVals: &sync.Map{}}
}

func (this *JobContext) UpdateVals(params map[string]interface{}) {
	this.Lock()
	defer this.Unlock()
	for paramName, paramVal := range params {
		this.GlobalVals.Store(paramName, paramVal)
	}
}

func (this *JobContext) GetVals(paramNames []string) (map[string]interface{}, error) {
	this.RLock()
	defer this.RUnlock()
	result := make(map[string]interface{})
	for _, paramName := range paramNames {
		if (paramName != "") {
			val, ok := this.GlobalVals.Load(paramName)
			if ok {
				result[paramName] = val
			} else {
				return nil, fmt.Errorf("param %s not found", paramName)
			}
		}
	}
	return result, nil
}

/**
一个计算任务
 */
type Job struct {
	Key        string
	Name       string
	jobContext *JobContext
}

/*
    执行dag job。
	每个dag task节点都有一个waitGroup，用于监听前序节点执行状态

	|task| ----------> |task| ----> |task| ----> |task|
	            \               /
                 ----> |task| ----> |task|
                /
    |task| ----------> |task| ----> |task| ----> |task|
    */
func (this *Job) Run(parentCtx context.Context, input map[string]interface{}) (output map[string]interface{}, err error) {
	if (parentCtx == nil) {
		parentCtx = context.Background()
	}

	defer func() {
		if err := recover(); err != nil {
			logs.Fatalf("libdag-job[%s]: job run panic ,reason: (%s)", this.Key, err)
		}
	}()

	err = this.ParseJobInputs(input)
	if (err != nil) {
		logs.Errorf("libdag-job[%s]: parse job input error ,reason: (%s)", this.Key, err)
		return nil, err
	}

	err = this.Schedule(parentCtx)
	if (err != nil) {
		logs.Errorf("libdag-job[%s]: parse job input error ,reason: (%s)", this.Key, err)
		return nil, err
	}

	output, err = this.ParseJobOutputs()

	if (err != nil) {
		logs.Errorf("libdag-job[%s]: parse job output error,reason: (%s)", this.Key, err)
		return nil, err
	}
	return output, nil
}

func (this *Job) ParseJobInputs(input map[string]interface{}) error {
	jobContext := this.jobContext
	inputs := make(map[string]interface{})
	for _, inputName := range jobContext.Config.Input {
		val, ok := input[inputName];
		if ok {
			inputs[inputName] = val
		} else {
			return fmt.Errorf("job input parse %s fail", inputName)
		}
	}

	jobContext.UpdateVals(inputs)
	return nil
}

func (this *Job) ParseJobOutputs() (map[string]interface{}, error) {
	jobContext := this.jobContext
	output, err := jobContext.GetVals(jobContext.Config.Output)
	if (err != nil) {
		return nil, fmt.Errorf("job output parse %s fail", err)
	}
	return output, nil
}

func (this *Job) Schedule(parentCtx context.Context) error {
	nodeConfs := this.jobContext.Config.Nodes
	tasks := make(map[string]*Task, len(nodeConfs))

	graph, err := ParseGraph(this.jobContext.Config)
	if (err != nil) {
		return err
	}

	sortedGraph, err := graph.Toposort() //拓扑排序，成环检测
	if (err != nil) {
		return err
	}

	//按拓扑顺序创建task
	for _, node := range sortedGraph {
		nodeConf := nodeConfs[node.Name]
		task, err := NewTask(nodeConf, this)
		if (err != nil) {
			return err
		}
		taskName := node.Name
		tasks[taskName] = task
		task.waitGroup = &sync.WaitGroup{}
	}

	//设定task之间的依赖关系
	for _, task := range tasks {
		node := graph.Nodes[task.Name]
		followers := node.Outputs;
		for it := followers.Front(); it != nil; it = it.Next() {
			followerName := it.Value.(string)
			followedTask := tasks[followerName]
			followedTask.waiting[task.Name] = task
			followedTask.waitGroup.Add(1)
			task.followers[followerName] = followedTask
		}
	}

	logs.Infof("libdag-job[%s]: job inited, %v task to run, wait job to finish", this.Key, len(tasks))

	latch := &sync.WaitGroup{}
	latch.Add(len(tasks))

	// 按拓扑顺序启动task，依赖关系靠task内置waitGroup状态触发，节点状态记录controlCode
	for _, task := range tasks {
		go task.Run(parentCtx, this.jobContext, func() {
			latch.Done()
		})
	}
	latch.Wait()
	logs.Infof("libdag-job[%s]: job finish", this.Key)
	return nil
}

func NewJob(jobKey string, jobContext *JobContext) (*Job, error) {
	if jobContext.Config == nil {
		return nil, errors.New("no dag config found")
	}
	return &Job{Key: jobKey, Name: jobContext.Config.Name, jobContext: jobContext}, nil
}
