// handlers.go
package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

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
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "获取文件链接失败"))
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

	commonPrefix := dirs[0]
	for _, d := range dirs[1:] {
		commonPrefix = longestCommonPrefix(commonPrefix, d)
	}
	if strings.HasSuffix(commonPrefix, "/") {
		commonPrefix = commonPrefix[:len(commonPrefix)-1]
	}

	state := &UserState{
		Step:          "upload_select_env",
		DstEnvs:       []string{},
		UploadRoot:    commonPrefix,
		UnzipPath:     unzipDir,
		ZipPath:       zipPath,
		ChatID:        msg.Chat.ID,
		MsgIDs:        []int{statusMsg.MessageID},
		AutoCreateAll: false,
	}

	stateLock.Lock()
	userStates[msg.Chat.ID] = state
	stateLock.Unlock()

	kb := buildEnvKeyboard(state)
	text := fmt.Sprintf("*ZIP 分析完成*\n\n检测到公共路径：`%s`\n顶级目录数量：%d 个\n\n请选择要部署的目标环境：", commonPrefix, len(dirs))
	clearAndSend(state, text, &kb)
}

func buildEnvKeyboard(state *UserState) tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	for _, env := range cfg.Environments {
		prefix := "○"
		if contains(state.DstEnvs, env.Name) {
			prefix = "● [Selected]"
		}
		btn := tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%s %s", prefix, env.Name),
			fmt.Sprintf("toggle_env|%s", env.Name))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(btn))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("确认部署 [Checkmark]", "confirm_deploy"),
	))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

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
	bot.Request(tgbotapi.NewCallback(cb.CallbackQuery.ID, ""))

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
			bot.Request(tgbotapi.NewCallback(cb.CallbackQuery.ID, "请至少选择一个环境"))
			return
		}
		bot.Request(tgbotapi.NewCallbackWithAlert(cb.CallbackQuery.ID, "开始部署..."))
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