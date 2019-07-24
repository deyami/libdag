package test

import (
	"github.com/deyami/libdag"
	"io/ioutil"
	"os"
)

type MockedLoader struct {
	ConfigPath string
}

func (this *MockedLoader) LoadConfigData(configKey string) ([]byte, error) {
	fd, err := os.Open(configKey + ".yaml")
	if err != nil {
		return nil, err
	}

	defer fd.Close()
	return ioutil.ReadAll(fd)
}

type MockedHandlerManager struct {
	libdag.HandlerManager
}


func (this *MockedHandlerManager) CreateHandler(handlerName string) (libdag.DagNodeHandler, error) {
	if (handlerName == "double") {
		return &Double{}, nil
	} else if (handlerName == "double1") {
		return &Double{}, nil
	}
	return nil, nil
}

type MockedConfigManager struct {
	libdag.ConfigManager
}

func (this *MockedConfigManager)GetConfig(configKey string) (*libdag.DagConfig, error) {
	configFileName := configKey + ".yaml"
	fd, err := os.Open(configFileName)
	if err != nil {
		return nil, err
	}

	defer fd.Close()
	configData, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, err
	}

	newConfig, err := libdag.ParseConfig(configData)
	if err != nil {
		return nil, err
	}
	return newConfig,nil
}