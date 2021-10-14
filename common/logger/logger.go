// expample
// package main
//
// import (
// 	"fmt"
// 	"time"
//
// 	log "logger"
//
// 	"go.uber.org/zap"
// )
//
// func main() {
// 	conf := log.LoggerConf{
// 		FilePath:    "./log/test1.log",
// 		IsLocalTime: true,
// 		MaxSize:     1024,
// 		MaxBackups:  2,
// 		MaxDays:     2,
// 		IsCompress:  true,
// 		ServerName:  "test",
// 	}
//
// 	logger, err := log.NewLogger(conf)
// 	if err != nil {
// 		fmt.Println("init logger failed")
// 		return
// 	}
//
// 	for {
// 		logger.Logger().Info("无法获取网址",
// 			zap.String("url", "http://www.baidu.com"),
// 			zap.Int("attempt", 3),
// 			zap.Duration("backoff", time.Second))
// 		time.Sleep(time.Second * 60)
// 	}
package logger

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LoggerConf struct {
	FilePath    string
	IsLocalTime bool
	MaxSize     int
	MaxBackups  int
	MaxDays     int
	IsCompress  bool
	Level       int

	ServerName string
}

type Logger struct {
	logger *zap.Logger
}

func (l *Logger) Logger() *zap.Logger {
	return l.logger
}
func (l *Logger) Sync() {
	l.logger.Sync()
}

func CustomTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

func NewLogger(conf LoggerConf) (*Logger, error) {
	if conf.FilePath == "" {
		return nil, fmt.Errorf("filepath is null")
	}
	if conf.MaxSize == 0 {
		conf.MaxSize = 1024
	}
	if conf.MaxBackups == 0 {
		conf.MaxBackups = 2
	}
	if conf.MaxDays == 0 {
		conf.MaxDays = 7
	}
	hook := lumberjack.Logger{
		Filename:   conf.FilePath,    // 日志文件路径
		LocalTime:  conf.IsLocalTime, //
		MaxSize:    conf.MaxSize,     // 每个日志文件保存的最大尺寸 单位：M
		MaxBackups: conf.MaxBackups,  // 日志文件最多保存多少个备份
		MaxAge:     conf.MaxDays,     // 文件最多保存多少天
		Compress:   conf.IsCompress,  // 是否压缩
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "linenum",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,      //
		EncodeLevel:    zapcore.LowercaseLevelEncoder,  // 小写编码器
		EncodeTime:     CustomTimeEncoder,              // 时间格式 1970-01-01 00:00:00.000
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.FullCallerEncoder,      // 全路径编码器
		EncodeName:     zapcore.FullNameEncoder,        //
	}

	// 设置日志级别
	atomicLevel := zap.NewAtomicLevel()
	switch conf.Level {
	case 1:
		atomicLevel.SetLevel(zap.DebugLevel)
	case 2:
		atomicLevel.SetLevel(zap.InfoLevel)
	case 3:
		atomicLevel.SetLevel(zap.WarnLevel)
	case 4:
		atomicLevel.SetLevel(zap.ErrorLevel)
	case 5:
		atomicLevel.SetLevel(zap.DPanicLevel)
	case 6:
		atomicLevel.SetLevel(zap.PanicLevel)
	case 7:
		atomicLevel.SetLevel(zap.FatalLevel)
	default:
		atomicLevel.SetLevel(zap.InfoLevel)
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),               // 编码器配置
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(&hook)), // 打印到控制台和文件
		atomicLevel, // 日志级别
	)

	// 开启开发模式，堆栈跟踪
	caller := zap.AddCaller()
	// 开启文件及行号
	development := zap.Development()
	// 设置初始化字段
	fields := zap.Fields(zap.String("ServerName", conf.ServerName))
	// 构造日志
	zaplogger := zap.New(core, caller, development, fields)

	logger := &Logger{
		logger: zaplogger,
	}

	return logger, nil
}
