// main.go
package main

import (
	"os"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var (
	bot        *tgbotapi.BotAPI
	log        = logrus.New()
	cfg        GlobalConfig
	taskLock   sync.Map
	userStates = make(map[int64]*UserState)
	stateLock  sync.RWMutex
)

type EnvConfig struct {
	Name   string `yaml:"name"`
	Region string `yaml:"region"`
	Bucket string `yaml:"bucket"`
	AK     string `yaml:"access_key_id"`
	SK     string `yaml:"secret_access_key"`
}

type GlobalConfig struct {
	TelegramToken string      `yaml:"telegram_token"`
	Admins        []int64     `yaml:"admins"`
	Environments  []EnvConfig `yaml:"environments"`
	Threads       int         `yaml:"threads"`
}

type UserState struct {
	Step          string
	SrcEnvs       []string
	DstEnvs       []string
	UploadRoot    string
	UnzipPath     string
	ZipPath       string
	ChatID        int64
	MsgIDs        []int
	AutoCreateAll bool
	mu            sync.Mutex
}

func main() {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		panic("无法读取 config.yaml: " + err.Error())
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		panic("config.yaml 解析失败: " + err.Error())
	}

	os.MkdirAll("uploads", 0755)
	os.MkdirAll("logs", 0755)

	logFile := time.Now().Format("logs/2006-01-02.log")
	f, _ := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	log.SetOutput(f)
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	log.SetLevel(logrus.InfoLevel)

	bot, err = tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		log.Fatal("Telegram Token 错误: ", err)
	}
	log.Infof("Bot 已启动: @%s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message != nil {
			go handleMessage(update.Message)
		}
		if update.CallbackQuery != nil {
			go handleCallback(update.CallbackQuery)
		}
	}
}

func handleMessage(msg *tgbotapi.Message) {
	if !isAdmin(msg.From.ID) {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "你没有权限使用此机器人"))
		return
	}

	if msg.Document != nil {
		if !strings.HasSuffix(strings.ToLower(msg.Document.FileName), ".zip") {
			bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "错误：只接受 .zip 压缩包"))
			return
		}
		go handleZipUpload(msg)
		return
	}

	text := strings.TrimSpace(msg.Text)
	if text == "/sync" {
		startSyncFlow(msg.Chat.ID)
		return
	}

	if strings.HasPrefix(text, "/start") || strings.HasPrefix(text, "/help") {
		help := "*可用功能：*\n" +
			"`/sync` - 多环境目录同步（保留所有标签、ACL、属性）\n" +
			"发送 `.zip` - 一键部署（强制打 public=yes 标签）"
		msgConfig := tgbotapi.NewMessage(msg.Chat.ID, help)
		msgConfig.ParseMode = "Markdown"
		bot.Send(msgConfig)
	}
}

func isAdmin(id int64) bool {
	for _, a := range cfg.Admins {
		if a == id {
			return true
		}
	}
	return false
}

func clearAndSend(state *UserState, text string, markup *tgbotapi.InlineKeyboardMarkup) *tgbotapi.Message {
	for _, id := range state.MsgIDs {
		bot.Request(tgbotapi.NewDeleteMessage(state.ChatID, id))
	}
	state.MsgIDs = state.MsgIDs[:0]

	msg := tgbotapi.NewMessage(state.ChatID, text)
	if markup != nil {
		msg.ReplyMarkup = markup
	}
	msg.ParseMode = "Markdown"
	sent, _ := bot.Send(msg)
	state.MsgIDs = append(state.MsgIDs, sent.MessageID)
	return &sent
}

func cleanupUserState(chatID int64) {
	time.AfterFunc(30*time.Minute, func() {
		stateLock.Lock()
		delete(userStates, chatID)
		stateLock.Unlock()
	})
}