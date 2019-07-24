package libdag

import (
	"github.com/deyami/libdag/utils/logs"
	"gopkg.in/yaml.v2"
)

type NodeConfig struct {
	Name      string                 `yaml:"name"`
	Labels    []string               `yaml:"labels"`
	Critical  bool                   `yaml:"critical"`
	Processor string                 `yaml:"processor"`
	Params    map[string]interface{} `yaml:"params"`
	Input     map[string]string      `yaml:"input"`
	Output    map[string]string      `yaml:"output"`
}

type DagConfig struct {
	Name   string                 `yaml:"name"`
	Input  []string               `yaml:"input"`
	Output []string               `yaml:"output"`
	Nodes  map[string]*NodeConfig `yaml:"nodes"`
}

type YamlConfig struct {
	DagConfig *DagConfig `yaml:"dag_config"`
}

func ParseConfig(configData []byte) (*DagConfig, error) {
	config := &YamlConfig{}

	err := yaml.Unmarshal(configData, config)
	if (err != nil) {
		logs.Errorf("libdag: parse config data error:%s", err)
		return nil, err
	}

	nodesConfig := config.DagConfig.Nodes

	for node_name, node := range nodesConfig {
		node.Name = node_name
		if (node.Params == nil) {
			node.Params = make(map[string]interface{})
		}
		if (node.Input == nil) {
			node.Input = make(map[string]string)
		}
		if (node.Output == nil) {
			node.Output = make(map[string]string)
		}
	}

	return config.DagConfig, nil
}
