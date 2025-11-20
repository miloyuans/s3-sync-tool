module s3-sync-tool

go 1.25

require (
    github.com/go-telegram-bot-api/telegram-bot-api/v5@latest
    get github.com/sirupsen/logrus@latest
    github.com/aws/aws-sdk-go-v2 v1.32.0
    github.com/aws/aws-sdk-go-v2/config v1.27.10
    github.com/aws/aws-sdk-go-v2/credentials v1.17.10
    github.com/aws/aws-sdk-go-v2/service/s3 v1.58.0
    github.com/aws/smithy-go v1.21.0
    github.com/mattn/go-colorable v0.1.13   // 美化日志颜色
    github.com/sirupsen/logrus v1.9.3
    github.com/vbauerster/mpb/v8 v8.7.2     // 进度条
    gopkg.in/yaml.v3 v3.0.1
)