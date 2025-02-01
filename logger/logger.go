package logger

import (
	"os"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger

func init() {
	logger = zerolog.New(os.Stdout).With().Timestamp().Logger().Level(zerolog.InfoLevel)
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
