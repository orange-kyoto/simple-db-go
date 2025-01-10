package logger

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger

func init() {
	os.MkdirAll("var", 0755)
	logFilePath := "var/simple-db-go." + time.Now().Format("20060102-150405") + ".log"
	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("ロギングのためのファイルを開くことができませんでした. err=%+v", err))
	}
	logger = zerolog.New(file).With().Timestamp().Logger()
}

func Debug(msg string) {
	logger.Debug().Msg(msg)
}

func Debugf(format string, args ...any) {
	logger.Debug().Msgf(format, args...)
}

func Info(msg string) {
	logger.Info().Msg(msg)
}

func Infof(format string, args ...any) {
	logger.Info().Msgf(format, args...)
}

func Warn(msg string) {
	logger.Warn().Msg(msg)
}

func Warnf(format string, args ...any) {
	logger.Warn().Msgf(format, args...)
}

func Error(msg string) {
	logger.Error().Msg(msg)
}

func Errorf(format string, args ...any) {
	logger.Error().Msgf(format, args...)
}
