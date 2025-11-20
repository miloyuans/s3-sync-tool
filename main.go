// main.go —— 企业级 Telegram + S3 多环境同步机器人（完整可编译版）
package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var (
	bot       *tgbotapi.BotAPI
	log       = logrus.New()
	cfg       GlobalConfig
	taskLock  sync.Map
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
	Step         string
	SrcEnvs      []string
	DstEnvs      []string
	SelectedDirs []string
	UploadDirs   []string
	UnzipPath    string
	ZipPath      string
	ChatID       int64
	MessageID    int
	DirCache     map[string][]string
	mu           sync.Mutex
}

type writerAtAdapter struct{ w *io.PipeWriter }

func (a writerAtAdapter) WriteAt(p []byte, _ int64) (int, error) {
	return a.w.Write(p)
}

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
		startSyncFlow(msg.Chat.ID)
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

// ==================== ZIP 上传 ====================
func handleZipUpload(msg *tgbotapi.Message) {
	taskID := fmt.Sprintf("zip_%d_%s", msg.From.ID, msg.Document.FileID)
	if _, loaded := taskLock.LoadOrStore(taskID, true); loaded {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "任务进行中，请勿重复上传"))
		return
	}
	defer taskLock.Delete(taskID)

	reply, _ := bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "正在接收并解压 ZIP 包..."))

	file, err := bot.GetFile(tgbotapi.FileConfig{FileID: msg.Document.FileID})
	if err != nil {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "获取文件失败"))
		return
	}
	url := bot.GetFileDirectURL(file.FileID)

	zipPath := fmt.Sprintf("uploads/zip_%d_%d.zip", msg.From.ID, time.Now().UnixNano())
	if err := downloadFile(url, zipPath); err != nil {
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

	stateLock.Lock()
	userStates[msg.Chat.ID] = &UserState{
		Step:       "upload_env",
		UploadDirs: dirs,
		UnzipPath:  unzipDir,
		ZipPath:    zipPath,
		ChatID:     msg.Chat.ID,
		MessageID:  reply.MessageID,
	}
	stateLock.Unlock()

	kb := buildEnvKeyboard("upload_env", msg.Chat.ID)
	text := fmt.Sprintf("ZIP 解压成功，发现目录：\n• %s\n\n请选择目标环境（可多选）", strings.Join(dirs, "\n• "))
	msgConfig := tgbotapi.NewMessage(msg.Chat.ID, text)
	msgConfig.ReplyMarkup = kb
	bot.Send(msgConfig)
}

// ==================== /sync 流程 ====================
func startSyncFlow(chatID int64) {
	stateLock.Lock()
	userStates[chatID] = &UserState{
		Step:     "src_env",
		ChatID:   chatID,
		DirCache: make(map[string][]string),
	}
	stateLock.Unlock()

	kb := buildEnvKeyboard("src_env", chatID)
	msg := tgbotapi.NewMessage(chatID, "第1步：请选择【源环境】（可多选）")
	msg.ReplyMarkup = kb
	bot.Send(msg)
}

// ==================== 回调处理 ====================
func handleCallback(cb *tgbotapi.CallbackQuery) {
	if !isAdmin(cb.From.ID) {
		return
	}

	data := cb.Data
	parts := strings.Split(data, "|")
	if len(parts) == 0 {
		return
	}
	action := parts[0]

	stateLock.RLock()
	state := userStates[cb.Message.Chat.ID]
	stateLock.RUnlock()
	if state == nil {
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if action == "toggle_env" && len(parts) > 1 {
		env := parts[1]
		if state.Step == "src_env" {
			toggleSlice(&state.SrcEnvs, env)
		} else {
			if contains(state.SrcEnvs, env) {
				bot.AnswerCallbackQuery(tgbotapi.NewCallback(cb.ID, "不能选择源环境"))
				return
			}
			toggleSlice(&state.DstEnvs, env)
		}
		kb := buildEnvKeyboard(state.Step, cb.Message.Chat.ID)
		text := getSelectionText(state)
		edit := tgbotapi.NewEditMessageTextAndMarkup(cb.Message.Chat.ID, cb.Message.MessageID, text, kb)
		bot.Send(edit)
	}

	if action == "next" {
		if (state.Step == "src_env" && len(state.SrcEnvs) == 0) ||
			((state.Step == "dst_env" || state.Step == "upload_env") && len(state.DstEnvs) == 0) {
			bot.AnswerCallbackQuery(tgbotapi.NewCallback(cb.ID, "请至少选择一个环境"))
			return
		}

		if state.Step == "src_env" {
			state.Step = "dst_env"
			kb := buildEnvKeyboard("dst_env", cb.Message.Chat.ID)
			edit := tgbotapi.NewEditMessageTextAndMarkup(cb.Message.Chat.ID, cb.Message.MessageID, "第2步：请选择【目标环境】（可多选）", kb)
			bot.Send(edit)
			return
		}

		if state.Step == "dst_env" {
			state.Step = "dirs"
			go loadAndShowDirs(state, cb)
			return
		}

		if state.Step == "upload_env" {
			bot.AnswerCallbackQuery(tgbotapi.NewCallbackWithAlert(cb.ID, "开始上传..."))
			go executeUpload(state)
			cleanupUserState(cb.Message.Chat.ID)
		}
	}

	if action == "confirm_sync" {
		if len(state.SelectedDirs) == 0 {
			bot.AnswerCallbackQuery(tgbotapi.NewCallback(cb.ID, "请至少选择一个目录"))
			return
		}
		bot.AnswerCallbackQuery(tgbotapi.NewCallbackWithAlert(cb.ID, "开始同步..."))
		go executeSync(state)
		cleanupUserState(cb.Message.Chat.ID)
	}
}

// ==================== 键盘构建 ====================
func buildEnvKeyboard(step string, chatID int64) tgbotapi.InlineKeyboardMarkup {
	stateLock.RLock()
	state := userStates[chatID]
	stateLock.RUnlock()

	var rows [][]tgbotapi.InlineKeyboardButton
	var selected []string
	if state != nil {
		if step == "src_env" {
			selected = state.SrcEnvs
		} else {
			selected = state.DstEnvs
		}
	}

	for _, env := range cfg.Environments {
		prefix := "○"
		if contains(selected, env.Name) {
			prefix = "●"
		}
		if step != "src_env" && state != nil && contains(state.SrcEnvs, env.Name) {
			prefix = "✕"
		}
		btn := tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%s %s", prefix, env.Name),
			fmt.Sprintf("toggle_env|%s", env.Name))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(btn))
	}

	nextBtn := tgbotapi.NewInlineKeyboardButtonData("下一步 →", "next")
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(nextBtn))

	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

func getSelectionText(state *UserState) string {
	if state.Step == "src_env" {
		return fmt.Sprintf("已选择源环境：%s", strings.Join(state.SrcEnvs, ", "))
	}
	return fmt.Sprintf("已选择目标环境：%s", strings.Join(state.DstEnvs, ", "))
}

// ==================== 目录加载与显示 ====================
func loadAndShowDirs(state *UserState, cb *tgbotapi.CallbackQuery) {
	msg, _ := bot.Send(tgbotapi.NewMessage(cb.Message.Chat.ID, "正在读取目录..."))

	allDirs := make(map[string][]string)
	for _, name := range state.SrcEnvs {
		env := findEnvByName(name)
		client, _, _ := newS3Client(env.Region, env.AK, env.SK)
		dirs := listTopLevelDirsS3(client, env.Bucket)
		allDirs[name] = dirs
		state.DirCache[name] = dirs
	}

	kb := buildDirKeyboard(state)
	text := "第3步：请选择要同步的目录（可多选）"
	edit := tgbotapi.NewEditMessageTextAndMarkup(msg.Chat.ID, msg.MessageID, text, kb)
	bot.Send(edit)
}

func buildDirKeyboard(state *UserState) tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	for envName, dirs := range state.DirCache {
		for _, dir := range dirs {
			display := fmt.Sprintf("%s → %s", envName, dir)
			prefix := "○"
			key := fmt.Sprintf("%s|%s", envName, dir)
			if contains(state.SelectedDirs, key) {
				prefix = "●"
			}
			btn := tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%s %s", prefix, display),
				fmt.Sprintf("select_dir|%s", key))
			rows = append(rows, tgbotapi.NewInlineKeyboardRow(btn))
		}
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("确认同步 →", "confirm_sync"),
	))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

// ==================== 执行任务 ====================

// ==================== 完整上传函数（ZIP 解压后上传）====================
func executeUpload(state *UserState) {
	chatID := state.ChatID
	successCount := 0
	failCount := 0

	for _, envName := range state.DstEnvs {
		env := findEnvByName(envName)
		if env == nil {
			continue
		}
		client, _, uploader := newS3Client(env.Region, env.AK, env.SK)

		for _, dir := range state.UploadDirs {
			localDir := filepath.Join(state.UnzipPath, dir)
			s3Prefix := dir + "/"

			// 检查目标目录是否存在
			exists := false
			paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
				Bucket: &env.Bucket,
				Prefix: &s3Prefix,
				MaxKeys: 1,
			})
			if paginator.HasMorePages() {
				if _, err := paginator.NextPage(context.Background()); err == nil {
					exists = true
				}
			}

			if !exists {
				// 询问是否创建目录
				kb := tgbotapi.NewInlineKeyboardMarkup(
					tgbotapi.NewInlineKeyboardRow(
						tgbotapi.NewInlineKeyboardButtonData("创建并上传", fmt.Sprintf("create_dir|%s|%s|%s", env.Name, dir, state.UnzipPath)),
						tgbotapi.NewInlineKeyboardButtonData("跳过", fmt.Sprintf("skip_dir|%s|%s", env.Name, dir)),
					),
				)
				bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("目录 [%s] 在 %s 不存在，是否创建？", dir, env.Name)).ReplyMarkup(kb))
				continue
			}

			// 直接上传
			count, errCount := uploadDirectoryWithPublicTag(client, uploader, localDir, env.Bucket, s3Prefix)
			successCount += count
			failCount += errCount
		}
	}

	bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("ZIP 上传完成！成功 %d 个，失败 %d 个", successCount, failCount)))
	cleanupFiles(state.ZipPath, state.UnzipPath)
}

// 上传本地目录（强制 public=yes）
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

// ==================== 完整同步函数（源 → 目标）====================
func executeSync(state *UserState) {
	chatID := state.ChatID
	totalSuccess := 0
	totalFail := 0

	for _, srcName := range state.SrcEnvs {
		srcEnv := findEnvByName(srcName)
		srcClient, srcDL, _ := newS3Client(srcEnv.Region, srcEnv.AK, srcEnv.SK)

		for _, dstName := range state.DstEnvs {
			dstEnv := findEnvByName(dstName)
			dstClient, _, dstUL := newS3Client(dstEnv.Region, dstEnv.AK, dstEnv.SK)

			for _, item := range state.SelectedDirs {
				parts := strings.Split(item, "|")
				if len(parts) != 2 {
					continue
				}
				srcPrefix := parts[1] + "/"

				success, fail := syncOneDirectory(srcClient, srcDL, dstClient, dstUL,
					srcEnv.Bucket, dstEnv.Bucket, srcPrefix, chatID)
				totalSuccess += success
				totalFail += fail
			}
		}
	}

	bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("同步完成！成功 %d 个，失败 %d 个", totalSuccess, totalFail)))
}

// 同步单个目录（完整逻辑）
func syncOneDirectory(srcClient *s3.Client, srcDL *manager.Downloader,
	dstClient *s3.Client, dstUL *manager.Uploader,
	srcBucket, dstBucket, prefix string, chatID int64) (int, int) {

	success := 0
	fail := 0

	paginator := s3.NewListObjectsV2Paginator(srcClient, &s3.ListObjectsV2Input{
		Bucket: &srcBucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			fail++
			continue
		}
		for _, obj := range page.Contents {
			if obj.Key == nil || strings.HasSuffix(*obj.Key, "/") {
				continue
			}
			key := *obj.Key
			rel := strings.TrimPrefix(key, prefix)
			targetKey := prefix + rel

			// 检查目标是否存在且 ETag 一致
			head, _ := dstClient.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: &dstBucket,
				Key:    &targetKey,
			})
			if head != nil && head.ETag != nil && obj.ETag != nil && *head.ETag == *obj.ETag {
				continue // 完全一致，跳过
			}

			// 冲突重命名
			if head != nil {
				targetKey = fmt.Sprintf("%s.sync%s", prefix+rel, time.Now().Format("20060102T150405Z"))
				log.Warnf("冲突重命名 → %s", targetKey)
			}

			// 流式传输
			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				_, err := srcDL.Download(context.Background(), writerAtAdapter{pw}, &s3.GetObjectInput{
					Bucket: &srcBucket,
					Key:    &key,
				})
				if err != nil {
					pw.CloseWithError(err)
				}
			}()

			_, err = dstUL.Upload(context.Background(), &s3.PutObjectInput{
				Bucket: &dstBucket,
				Key:    &targetKey,
				Body:   pr,
			})
			if err != nil {
				log.Errorf("同步失败 %s → %s: %v", key, targetKey, err)
				fail++
			} else {
				success++
			}

			// 复制标签
			tagResp, _ := srcClient.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{
				Bucket: &srcBucket,
				Key:    &key,
			})
			tags := tagResp.TagSet
			if len(tags) == 0 {
				tags = append(tags, types.Tag{Key: aws.String("public"), Value: aws.String("yes")})
			}
			if len(tags) > 0 {
				dstClient.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
					Bucket:  &dstBucket,
					Key:     &targetKey,
					Tagging: &types.Tagging{TagSet: tags},
				})
			}
		}
	}
	return success, fail
}

// ==================== S3 工具函数 ====================
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

func listTopLevelDirsS3(client *s3.Client, bucket string) []string {
	var dirs []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    &bucket,
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, _ := paginator.NextPage(context.Background())
		for _, p := range page.CommonPrefixes {
			if p.Prefix != nil {
				dir := strings.TrimSuffix(*p.Prefix, "/")
				dirs = append(dirs, dir)
			}
		}
	}
	return dirs
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

// ==================== 辅助函数 ====================
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