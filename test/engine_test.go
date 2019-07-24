package test

import (
	"context"
	"fmt"
	"github.com/deyami/libdag"
	"github.com/demdxx/gocast"
	"testing"
	"time"
)

func mockJobInputs() map[string]interface{} {
	jobInputs := make(map[string]interface{})
	jobInputs["a"] = 1
	return jobInputs
}

func TestJob(t *testing.T) {
	configManager := &MockedConfigManager{}
	dagConfig, err := configManager.GetConfig("dag1")
	if (err != nil) {
		t.Errorf("parse config err %s", err)
		return
	}

	registry := &MockedHandlerManager{}

	jobContext := libdag.NewJobContext(dagConfig, registry)

	job, err := libdag.NewJob("dag1", jobContext)
	if (err != nil) {
		t.Errorf("parse config err %s", err)
		return
	}

	output, err := job.Run(nil, mockJobInputs())
	if (err != nil) {
		t.Errorf("parse config err %s", err)
		return
	}
	if (2 != gocast.ToInt32(output["doubled"])) {
		t.Errorf("caculate error expect:%v ,actutal:%v", 2, output["doubled"])
	}

	if (4 != gocast.ToInt32(output["doubled2"])) {
		t.Errorf("caculate error expect:%v ,actutal:%v", 4, output["doubled2"])
	}

	fmt.Printf("output %s \n", output)
}

func TestJobTimeout(t *testing.T) {
	configManager := &MockedConfigManager{}
	dagConfig, err := configManager.GetConfig("dag1")
	if (err != nil) {
		t.Errorf("parse config err %s", err)
		return
	}

	registry := &MockedHandlerManager{}

	jobContext := libdag.NewJobContext(dagConfig, registry)

	job, err := libdag.NewJob("dag1", jobContext)
	if (err != nil) {
		t.Errorf("parse config err %s", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1*time.Microsecond))
	defer cancel()

	output, err := job.Run(ctx, mockJobInputs())
	if (err != nil) {
		t.Errorf("parse execute err %s", err)
		return
	}
	if (2 != gocast.ToInt32(output["doubled"])) {
		t.Errorf("caculate error expect:%v ,actutal:%v", 2, output["doubled"])
	}

	if (4 != gocast.ToInt32(output["doubled2"])) {
		t.Errorf("caculate error expect:%v ,actutal:%v", 4, output["doubled2"])
	}
	fmt.Printf("output %s", output)
}



func TestCircularJob(t *testing.T) {
	configManager := &MockedConfigManager{}
	dagConfig, err := configManager.GetConfig("dag_circular")
	if (err != nil) {
		t.Errorf("parse config err %s", err)
		return
	}

	registry := &MockedHandlerManager{}

	jobContext := libdag.NewJobContext(dagConfig, registry)

	job, err := libdag.NewJob("dag1", jobContext)
	if (err != nil) {
		t.Errorf("parse config err %s", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1*time.Microsecond))
	defer cancel()

	output, err := job.Run(ctx, mockJobInputs())
	if (err != nil) {
		t.Errorf("parse execute err %s", err)
		return
	}
	if (2 != gocast.ToInt32(output["doubled"])) {
		t.Errorf("caculate error expect:%v ,actutal:%v", 2, output["doubled"])
	}

	if (4 != gocast.ToInt32(output["doubled2"])) {
		t.Errorf("caculate error expect:%v ,actutal:%v", 4, output["doubled2"])
	}
	fmt.Printf("output %s", output)
}


