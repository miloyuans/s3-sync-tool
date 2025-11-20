// main.go —— 企业级 S3 + Telegram 多环境部署机器人（完整终极版）
// 支持：无限层级目录浏览、分页、自动路径匹配、自动创建目录、对话自动清理、最终仅一条总结
package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	DstEnvs       []string          // 目标环境
	UploadRoot    string            // ZIP 中识别出的公共前缀，如 admin/v3/
	UnzipPath     string            // 解压目录
	ZipPath       string            // 原始ZIP路径
	ChatID        int64
	MsgIDs        []int             // 所有交互消息ID，用于清理
	AutoCreateAll bool              // 是否已确认全部自动创建目录
	mu            sync.Mutex
}

const PageSize = 10

func main() {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		panic("无法读取 config.yaml: " + err.Error())
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		panic("config.yaml 解析失败: " + err.Error())
	}
	if cfg.Threads <= 0 {
		cfg.Threads = 100
	}

	os.MkdirAll("uploads", 0755)
	os.MkdirAll("logs", 0755)

	logFile := fmt.Sprintf("logs/%s.log", time.Now().Format("2006-01-02"))
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

	if strings.HasPrefix(msg.Text, "/sync") {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "暂未实现 /sync 同步功能（本版专注一键部署）"))
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

// ==================== 消息清理工具 ====================
func deleteMessages(chatID int64, msgIDs []int) {
	for _, id := range msgIDs {
		bot.Request(tgbotapi.NewDeleteMessage(chatID, id))
	}
}

func addMsgID(state *UserState, msgID int) {
	state.MsgIDs = append(state.MsgIDs, msgID)
}

func clearAndSend(state *UserState, text string, markup *tgbotapi.InlineKeyboardMarkup) *tgbotapi.Message {
	deleteMessages(state.ChatID, state.MsgIDs)
	state.MsgIDs = state.MsgIDs[:0]
	msg := tgbotapi.NewMessage(state.ChatID, text)
	if markup != nil {
		msg.ReplyMarkup = markup
	}
	msg.ParseMode = "Markdown"
	sent, _ := bot.Send(msg)
	addMsgID(state, sent.MessageID)
	return sent
}

// ==================== ZIP 智能上传主流程 ====================
func handleZipUpload(msg *tgbotapi.Message) {
	taskID := fmt.Sprintf("zip_%d_%s", msg.From.ID, msg.Document.FileID)
	if _, loaded := taskLock.LoadOrStore(taskID, true); loaded {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "任务进行中，请勿重复上传"))
		return
	}
	defer taskLock.Delete(taskID)

	statusMsg, _ := bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "正在下载并分析 ZIP 包..."))

	fileLink, err := bot.GetFileDirectURL(msg.Document.FileID)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "获取文件失败"))
		return
	}

	zipPath := fmt.Sprintf("uploads/zip_%d_%d.zip", msg.From.ID, time.Now().UnixNano())
	if err := downloadFile(fileLink, zipPath); err != nil {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "下载失败"))
		return
	}

	unzipDir := fmt.Sprintf("uploads/unzipped_%d_%d", msg.From.ID, time.Now().UnixNano())
	os.MkdirAll(unzipDir, 0755)
	if err := unzip(zipPath, unzipDir); err != nil {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "解压失败"))
		cleanupFiles(zipPath, unzipDir)
		return
	}

	dirs := listTopLevelDirsLocal(unzipDir)
	if len(dirs) == 0 {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "ZIP 包内无有效目录"))
		cleanupFiles(zipPath, unzipDir)
		return
	}

	// 智能识别最长公共前缀
	commonPrefix := dirs[0]
	for _, d := range dirs[1:] {
		commonPrefix = longestCommonPrefix(commonPrefix, d)
	}
	if strings.HasSuffix(commonPrefix, "/") {
		commonPrefix = commonPrefix[:len(commonPrefix)-1]
	}

	state := &UserState{
		Step:       "upload_select_env",
		DstEnvs:    []string{},
		UploadRoot: commonPrefix,
		UnzipPath:  unzipDir,
		ZipPath:    zipPath,
		ChatID:     msg.Chat.ID,
		MsgIDs:     []int{statusMsg.MessageID},
	}

	stateLock.Lock()
	userStates[msg.Chat.ID] = state
	stateLock.Unlock()

	kb := buildEnvKeyboard(state)
	text := fmt.Sprintf("*ZIP 分析完成*\n\n检测到公共路径：`%s`\n目录数量：%d 个\n\n请勾选要部署的目标环境：", commonPrefix, len(dirs))
	clearAndSend(state, text, &kb)
}

// ==================== 环境选择键盘 ====================
func buildEnvKeyboard(state *UserState) tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	for _, env := range cfg.Environments {
		prefix := "○"
		if contains(state.DstEnvs, env.Name) {
			prefix = "●"
		}
		btn := tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%s %s", prefix, env.Name),
			fmt.Sprintf("toggle_env|%s", env.Name))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(btn))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("确认部署 →", "confirm_deploy"),
	))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

// ==================== 回调处理 ====================
func handleCallback(cb *tgbotapi.CallbackQuery) {
	if !isAdmin(cb.From.ID) {
		return
	}

	stateLock.RLock()
	state := userStates[cb.Message.Chat.ID]
	stateLock.RUnlock()
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()

	data := cb.Data
	parts := strings.Split(data, "|")
	if len(parts) == 0 {
		return
	}
	action := parts[0]
	bot.Request(tgbotapi.NewCallback(cb.ID, ""))

	if action == "toggle_env" && len(parts) > 1 {
		toggleSlice(&state.DstEnvs, parts[1])
		kb := buildEnvKeyboard(state)
		text := fmt.Sprintf("已选择部署环境（%d个）：%s", len(state.DstEnvs), strings.Join(state.DstEnvs, ", "))
		edit := tgbotapi.NewEditMessageTextAndMarkup(cb.Message.Chat.ID, cb.Message.MessageID, text, kb)
		edit.ParseMode = "Markdown"
		bot.Send(edit)
	}

	if action == "confirm_deploy" {
		if len(state.DstEnvs) == 0 {
			bot.Request(tgbotapi.NewCallback(cb.ID, "请至少选择一个环境"))
			return
		}
		bot.Request(tgbotapi.NewCallbackWithAlert(cb.ID, "开始部署..."))
		go executeSmartUpload(state)
	}

	if action == "create_all" {
		state.AutoCreateAll = true
		go executeSmartUpload(state)
	}

	if action == "cancel_deploy" {
		cleanupFiles(state.ZipPath, state.UnzipPath)
		clearAndSend(state, "已取消本次部署，所有临时文件已清理。", nil)
		cleanupUserState(cb.Message.Chat.ID)
	}
}

// ==================== 智能部署核心函数 ====================
func executeSmartUpload(state *UserState) {
	summary := "*部署总结*\n\n"
	successTotal := 0
	failTotal := 0
	createdDirs := 0

	for _, envName := range state.DstEnvs {
		env := findEnvByName(envName)
		if env == nil {
			continue
		}
		client, _, uploader := newS3Client(env.Region, env.AK, env.SK)
		localRoot := filepath.Join(state.UnzipPath, state.UploadRoot)
		s3Prefix := state.UploadRoot + "/"

		// 检查路径是否存在
		exists := dirExistsS3(client, env.Bucket, s3Prefix)
		if !exists && !state.AutoCreateAll {
			kb := tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("全部自动创建并上传", "create_all"),
					tgbotapi.NewInlineKeyboardButtonData("取消本次部署", "cancel_deploy"),
				),
			)
			text := fmt.Sprintf("目标路径不存在\n\n环境：*%s*\n路径：`%s`\n\n是否自动创建并上传？", envName, s3Prefix)
			clearAndSend(state, text, &kb)
			return
		}
		if !exists {
			createdDirs++
		}

		success, fail := uploadDirectoryWithPublicTag(client, uploader, localRoot, env.Bucket, s3Prefix)
		successTotal += success
		failTotal += fail
		summary += fmt.Sprintf("• %s：成功 %d，失败 %d\n", envName, success, fail)
	}

	if createdDirs > 0 {
		summary += fmt.Sprintf("\n已自动创建 %d 个目录\n", createdDirs)
	}
	summary += fmt.Sprintf("\n*总计*：成功 %d 个文件，失败 %d 个", successTotal, failTotal)

	cleanupFiles(state.ZipPath, state.UnzipPath)
	clearAndSend(state, summary, nil)
	cleanupUserState(state.ChatID)
}

// ==================== S3 & 文件工具函数 ====================
func uploadDirectoryWithPublicTag(client *s3.Client, uploader *manager.Uploader, localDir, bucket, prefix string) (int, int) {
	success := 0
	fail := 0

	filepath.WalkDir(localDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(localDir, path)
		key := prefix + rel

		f, err := os.Open(path)
		if err != nil {
			fail++
			return nil
		}
		defer f.Close()

		_, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket:  &bucket,
			Key:     &key,
			Body:    f,
			Tagging: aws.String("public=yes"),
		})
		if err != nil {
			log.Errorf("上传失败 %s: %v", key, err)
			fail++
		} else {
			success++
		}
		return nil
	})
	return success, fail
}

func dirExistsS3(client *s3.Client, bucket, prefix string) bool {
	resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:  &bucket,
		Prefix:  &prefix,
		MaxKeys: aws.Int32(1),
	})
	return err == nil && (len(resp.Contents) > 0 || len(resp.CommonPrefixes) > 0)
}

func newS3Client(region, ak, sk string) (*s3.Client, *manager.Downloader, *manager.Uploader) {
	cfg, _ := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	client := s3.NewFromConfig(cfg)
	return client, manager.NewDownloader(client), manager.NewUploader(client)
}

func findEnvByName(name string) *EnvConfig {
	for _, e := range cfg.Environments {
		if e.Name == name {
			return &e
		}
	}
	return nil
}

func listTopLevelDirsLocal(path string) []string {
	var dirs []string
	filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || p == path {
			return nil
		}
		rel, _ := filepath.Rel(path, p)
		if !strings.Contains(rel, string(filepath.Separator)) {
			dirs = append(dirs, d.Name())
		}
		return filepath.SkipDir
	})
	return dirs
}

func longestCommonPrefix(a, b string) string {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	for i > 0 && a[i-1] != '/' {
		i--
	}
	return a[:i]
}

func downloadFile(url, path string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	f, _ := os.Create(path)
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()
	for _, f := range r.File {
		path := filepath.Join(dest, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(path, 0755)
			continue
		}
		os.MkdirAll(filepath.Dir(path), 0755)
		out, _ := os.Create(path)
		rc, _ := f.Open()
		io.Copy(out, rc)
		out.Close()
		rc.Close()
	}
	return nil
}

func cleanupFiles(zipPath, dir string) {
	os.Remove(zipPath)
	os.RemoveAll(dir)
}

func cleanupUserState(chatID int64) {
	time.AfterFunc(30*time.Minute, func() {
		stateLock.Lock()
		delete(userStates, chatID)
		stateLock.Unlock()
	})
}

func toggleSlice(slice *[]string, item string) {
	for i, v := range *slice {
		if v == item {
			*slice = append((*slice)[:i], (*slice)[i+1:]...)
			return
		}
	}
	*slice = append(*slice, item)
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}