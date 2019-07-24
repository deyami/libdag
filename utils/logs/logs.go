package logs

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	//TODO change this conf as you wish
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.WarnLevel)
}


func Errorf(f string, v ...interface{}) {
	log.Errorf(f,v)
	return
}

func Warnf(f string, v ...interface{}) {
	log.Warnf(f,v)
	return
}

func Fatalf(f string, v ...interface{}) {
	log.Fatalf(f,v)
	return
}

func Warningf(f string, v ...interface{}) {
	log.Warningf(f,v)
	return
}

func Infof(f string, v ...interface{}) {
	log.Infof(f,v)
	return
}

func Debugf(f string, v ...interface{}) {
	log.Debugf(f,v)
	return
}
