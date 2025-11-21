// handlers.go
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// /sync 流程
func startSyncFlow(chatID int64) {
	state := &UserState{
		Step:    "src_env",
		SrcEnvs: []string{},
		DstEnvs: []string{},
		ChatID:  chatID,
		MsgIDs:  []int{},
	}
	stateLock.Lock()
	userStates[chatID] = state
	stateLock.Unlock()

	kb := buildEnvKeyboard(state, "src_env")
	clearAndSend(state, "*第1步：请选择【源环境】（可多选）*", &kb)
}

func buildEnvKeyboard(state *UserState, step string) tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	selected := state.SrcEnvs
	if step == "dst_env" || step == "upload_select_env" {
		selected = state.DstEnvs
	}

	for _, env := range cfg.Environments {
		mark := "○"
		if contains(selected, env.Name) {
			mark = "●"
		}
		if step == "dst_env" && contains(state.SrcEnvs, env.Name) {
			mark = "✕"
		}
		btn := tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%s %s", mark, env.Name),
			fmt.Sprintf("toggle_env|%s|%s", step, env.Name))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(btn))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("下一步", "next_step"),
	))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

// ZIP 上传
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
	downloadFile(fileLink, zipPath)

	unzipDir := fmt.Sprintf("uploads/unzipped_%d_%d", msg.From.ID, time.Now().UnixNano())
	os.MkdirAll(unzipDir, 0755)
	unzip(zipPath, unzipDir)

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
		Step:        "upload_select_env",
		DstEnvs:     []string{},
		UploadRoot:  commonPrefix,
		UnzipPath:   unzipDir,
		ZipPath:     zipPath,
		ChatID:      msg.Chat.ID,
		MsgIDs:      []int{statusMsg.MessageID},
	}

	stateaniiLock.Lock()
	userStates[msg.Chat.ID] = state
	stateLock.Unlock()

	kb := buildEnvKeyboard(state, "upload_select_env")
	text := fmt.Sprintf("*ZIP 分析完成*\n\n公共路径：`%s`\n目录数：%d\n\n请选择部署目标环境：", commonPrefix, len(dirs))
	clearAndSend(state, text, &kb)
}

// 统一回调处理
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
	action := parts[0]
	bot.Request(tgbotapi.NewCallback(cb.ID, ""))

	if action == "toggle_env" && len(parts) >= 3 {
		step := parts[1]
		env := parts[2]
		if step == "src_env" {
			toggleSlice(&state.SrcEnvs, env)
		} else {
			if step == "dst_env" && contains(state.SrcEnvs, env) {
				bot.Request(tgbotapi.NewCallback(cb.ID, "不能选源环境"))
				return
			}
			toggleSlice(&state.DstEnvs, env)
		}
		kb := buildEnvKeyboard(state, step)
		text := "已选择"
		if step == "src_env" {
			text = fmt.Sprintf("源环境（%d个）：%s", len(state.SrcEnvs), strings.Join(state.SrcEnvs, ", "))
		} else {
			text = fmt.Sprintf("目标环境（%d个）：%s", len(state.DstEnvs), strings.Join(state.DstEnvs, ", "))
		}
		edit := tgbotapi.NewEditMessageTextAndMarkup(cb.Message.Chat.ID, cb.Message.MessageID, text, kb)
		edit.ParseMode = "Markdown"
		bot.Send(edit)
	}

	if action == "next_step" {
		if state.Step == "src_env" && len(state.SrcEnvs) == 0 {
			bot.Request(tgbotapi.NewCallback(cb.ID, "请选择源环境"))
			return
		}
		if state.Step == "src_env" {
			state.Step = "dst_env"
			kb := buildEnvKeyboard(state, "dst_env")
			clearAndSend(state, "*第2步：请选择【目标环境】（可多选）*", &kb)
			return
		}
		if state.Step == "dst_env" && len(state.DstEnvs) == 0 {
			bot.Request(tgbotapi.NewCallback(cb.ID, "请选择目标环境"))
			return
		}
		if state.Step == "dst_env" {
			bot.Request(tgbotapi.NewCallbackWithAlert(cb.ID, "开始同步..."))
			go executeSync(state)
			cleanupUserState(state.ChatID)
		}
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
		clearAndSend(state, "已取消部署", nil)
		cleanupUserState(cb.Message.Chat.ID)
	}
}

// /sync 执行
func executeSync(state *UserState) {
	summary := "*同步完成*\n\n"
	totalSuccess := 0
	totalFail := 0

	for _, srcName := range state.SrcEnvs {
		srcEnv := findEnvByName(srcName)
		srcClient, srcDL, _ := newS3Client(srcEnv.Region, srcEnv.AK, srcEnv.SK)

		for _, dstName := range state.DstEnvs {
			dstEnv := findEnvByName(dstName)
			dstClient, _, dstUL := newS3Client(dstEnv.Region, dstEnv.AK, dstEnv.SK)

			success, fail := syncOneDirectory(srcClient, srcDL, dstClient, dstUL,
				srcEnv.Bucket, dstEnv.Bucket, "", state.ChatID)
			totalSuccess += success
			totalFail += fail
			summary += fmt.Sprintf("• %s → %s：成功 %d，失败 %d\n", srcName, dstName, success, fail)
		}
	}
	summary += fmt.Sprintf("\n*总计*：成功 %d，失败 %d", totalSuccess, totalFail)
	clearAndSend(state, summary, nil)
}

// ZIP 部署执行（保留原 public=yes 逻辑）
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
					tgbotapi.NewInlineKeyboardButtonData("全部创建", "create_all"),
					tgbotapi.NewInlineKeyboardButtonData("取消", "cancel_deploy"),
				),
			)
			clearAndSend(state, fmt.Sprintf("路径 `%s` 在 %s 不存在\n是否创建？", s3Prefix, envName), &kb)
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
		summary += fmt.Sprintf("\n已创建 %d 个目录\n", createdDirs)
	}
	summary += fmt.Sprintf("\n*总计*：成功 %d，失败 %d", successTotal, failTotal)
	cleanupFiles(state.ZipPath, state.UnzipPath)
	clearAndSend(state, summary, nil)
	cleanupUserState(state.ChatID)
}