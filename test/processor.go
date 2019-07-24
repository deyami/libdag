package test

import (
	"github.com/deyami/libdag/utils/logs"
	"context"
	"github.com/demdxx/gocast"
)




type Double struct {
}

func (this *Double) Init(parentCtx context.Context, params map[string]interface{}) error {
	logs.Infof("Init params :%s", params)
	return nil
}

func (this *Double) Process(ctx context.Context, args map[string]interface{}) (map[string]interface{}, error) {
	A := gocast.ToInt32(args["a"])
	result := make(map[string]interface{})
	result["b"] = 2*A
	return result, nil
}
