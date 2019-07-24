package libdag

import (
	"context"
	"fmt"
	"github.com/deyami/libdag/utils/logs"
	"sync"
)

type CONTROL_CODE int

const (
	EXECUTION_CONTINUE CONTROL_CODE = iota
	EXECUTION_SKIP_FOLLOWER_TASK
	EXECUTION_STOP_JOB
)

type Task struct {
	Key         string
	Name        string
	params      map[string]interface{}
	input       map[string]string
	output      map[string]string
	handler     DagNodeHandler
	waiting     map[string]*Task
	followers   map[string]*Task
	waitGroup   *sync.WaitGroup
	controlCode CONTROL_CODE
}

func (this *Task) Run(parentCtx context.Context, jobContext *JobContext, callback func()) {
	defer callback()
	defer this.notifyFollower(parentCtx)

	logs.Debugf("libdag-task[%s]: start task", this.Key)
	this.WaitPre() //等待前序节点执行完毕
	logs.Debugf("libdag-task[%s]: run task", this.Key)

	select {
	case <-parentCtx.Done():
		this.controlCode = EXECUTION_STOP_JOB
	default:
		this.RealRun(parentCtx, jobContext)
	}
}

/**
执行任务
 */
func (this *Task) RealRun(parentCtx context.Context, jobContext *JobContext) {
	defer func() {
		if err := recover(); err != nil {
			logs.Fatalf("libdag-task[%s]: execute task painc,reason: (%s)", this.Key, err)
			this.controlCode = EXECUTION_STOP_JOB //task panic,整个job停止
		}
	}()

	preControlCode := this.suggestControlCode()

	if preControlCode == EXECUTION_STOP_JOB { //1.前序节点建议任务取消
		this.controlCode = EXECUTION_STOP_JOB
	} else if preControlCode == EXECUTION_SKIP_FOLLOWER_TASK { //2.前序节点建议跳过后续流程
		this.controlCode = EXECUTION_SKIP_FOLLOWER_TASK
	} else if preControlCode == EXECUTION_CONTINUE { //3.前序节点执行正常
		inputs, err := this.ParseTaskInputs(jobContext)
		if err != nil {
			logs.Errorf("libdag-task[%s]: parse handler input error %s", this.Key, err)
			this.controlCode = EXECUTION_SKIP_FOLLOWER_TASK
			return
		}

		err = this.handler.Init(parentCtx, this.params)
		if err != nil {
			logs.Errorf("libdag-task[%s]: init handler error,reason: (%s)", this.Key, err)
			this.controlCode = EXECUTION_STOP_JOB
			return
		}

		taskOutput, err := this.handler.Process(parentCtx, inputs)
		if err != nil {
			logs.Errorf("libdag-task[%s]: run handler error,reason: (%s)", this.Key, err)
			this.controlCode = EXECUTION_SKIP_FOLLOWER_TASK
			return
		}
		err = this.ParseTaskOutputs(taskOutput, jobContext)
		if err != nil {
			logs.Errorf("libdag-task[%s]: parse handler output error,reason: (%s)", this.Key, err)
			this.controlCode = EXECUTION_SKIP_FOLLOWER_TASK
			return
		}
		this.controlCode = EXECUTION_CONTINUE
		return
	}
}

func (this *Task) ParseTaskInputs(context *JobContext) (map[string]interface{}, error) {
	contextNames := make([]string, 0, len(this.input))
	for _, contextName := range this.input {
		contextNames = append(contextNames, contextName)
	}

	contextVals, err := context.GetVals(contextNames)
	if (err != nil) {
		return nil, err
	}

	taskInput := make(map[string]interface{}, len(contextVals))
	for inputName, contextName := range this.input {
		taskInput[inputName] = contextVals[contextName]
	}

	return taskInput, nil
}

func (this *Task) ParseTaskOutputs(output map[string]interface{}, context *JobContext) error {
	if (output == nil) {
		return fmt.Errorf("error occured when outject args to task context,reason: output is nil")
	}

	contextVals := make(map[string]interface{}, 0)

	for contextName, outputName := range this.output {
		contextVals[contextName] = output[outputName]
	}

	context.UpdateVals(contextVals)
	return nil
}

/**
通知后继节点
 */
func (this *Task) notifyFollower(parentCtx context.Context) {
	for _, follower := range this.followers {
		logs.Debugf("libdag-task[%s]: task finish, notify follower(%s)", this.Key, follower.Key)
		follower.waitGroup.Done()
	}
}

/**
根据所有前序task执行结果决定job是否继续执行
TODO 区分强依赖与弱依赖
 */
func (this *Task) suggestControlCode() CONTROL_CODE {
	for _, pre := range this.waiting {
		if (pre.controlCode == EXECUTION_SKIP_FOLLOWER_TASK) {
			return EXECUTION_SKIP_FOLLOWER_TASK
		} else if (pre.controlCode == EXECUTION_STOP_JOB) {
			return EXECUTION_STOP_JOB
		}
	}
	return EXECUTION_CONTINUE
}

/**
等待前序节点完成
 */
func (this *Task) WaitPre() {
	this.waitGroup.Wait()
}

func NewTask(node *NodeConfig, job *Job) (*Task, error) {
	task := &Task{}
	task.Name = node.Name
	task.Key = job.Key + "-" + task.Name
	task.params = node.Params
	task.input = node.Input
	task.output = node.Output
	handler, err := job.jobContext.HandlerRegistry.CreateHandler(node.Processor)
	if (err != nil) {
		return nil, err
	}
	task.handler = handler
	task.waiting = map[string]*Task{}
	task.followers = map[string]*Task{}
	return task, nil
}
