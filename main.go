package nano

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"os"
)

func RegisterModule(name string) Module {
	if len(name) < 1 {
		panic(errors.New("Module's name cannot be empty"))
	}

	m, err := newModule(name)
	if err != nil {
		panic(err)
	}

	if os.Getenv("ENV") == "production" {
		log.SetLevel(log.WarnLevel)
	} else {
		log.SetLevel(log.DebugLevel)
	}

	log.SetOutput(os.Stderr)

	m.Log.Info("Module registered")

	return *m
}
