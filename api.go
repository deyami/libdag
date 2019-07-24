package libdag

import (
	"context"
)

/**
配置管理器
 */
type ConfigManager interface {
	GetConfig(configKey string) (*DagConfig, error)
}

/**
dag节点处理器
 */
type DagNodeHandler interface {
	Init(parentCtx context.Context, params map[string]interface{}) error //初始化方法
	Process(parentCtx context.Context, input map[string]interface{}) (map[string]interface{}, error)
}

/**
dag节点管理器
 */
type HandlerManager interface {
	CreateHandler(handlerName string) (DagNodeHandler, error)
}
