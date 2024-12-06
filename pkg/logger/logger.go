package logger

import (
    "os"

    "github.com/sirupsen/logrus"
)

var Log = logrus.New()

func InitLogger() *logrus.Logger {
    logger := logrus.New()
    logger.Out = os.Stdout
    logger.SetLevel(logrus.InfoLevel)
    logger.SetFormatter(&logrus.TextFormatter{
        FullTimestamp: true,
    })
    Log = logger
    return logger
}