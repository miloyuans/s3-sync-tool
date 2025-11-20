// main.go —— 生产级 S3 跨账户同步工具（兼容所有 mpb/v8 版本）
package main

import (
        "context"
        "fmt"
        "io"
        "os"
        "strings"
        "sync"
        "time"

        "github.com/aws/aws-sdk-go-v2/aws"
        "github.com/aws/aws-sdk-go-v2/config"
        "github.com/aws/aws-sdk-go-v2/credentials"
        "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
        "github.com/aws/aws-sdk-go-v2/service/s3"
        "github.com/aws/aws-sdk-go-v2/service/s3/types"
        "github.com/sirupsen/logrus"
        "github.com/vbauerster/mpb/v8"
        "github.com/vbauerster/mpb/v8/decor"
        "gopkg.in/yaml.v3"
)

var log = logrus.New()

// 配置结构（保持不变）
type Source struct {
        Name            string `yaml:"name"`
        Region          string `yaml:"region"`
        Bucket          string `yaml:"bucket"`
        Prefix          string `yaml:"prefix"`
        AccessKeyID     string `yaml:"access_key_id"`
        SecretAccessKey string `yaml:"secret_access_key"`
}

type Destination struct {
        Name            string `yaml:"name"`
        Region          string `yaml:"region"`
        Bucket          string `yaml:"bucket"`
        Prefix          string `yaml:"prefix"`
        AccessKeyID     string `yaml:"access_key_id"`
        SecretAccessKey string `yaml:"secret_access_key"`
}

type MappingRule struct {
        SourcePrefix string `yaml:"source_prefix"`
        TargetPrefix string `yaml:"target_prefix"`
}

type Config struct {
        Sources           []Source          `yaml:"sources"`
        Destinations      []Destination     `yaml:"destinations"`
        DirectoryMappings []MappingRule     `yaml:"directory_mappings"`
        DefaultTags       map[string]string `yaml:"default_tags"`
        Threads           int               `yaml:"threads"`
        LogFile           string            `yaml:"log_file"`
}

type writerAtAdapter struct{ w *io.PipeWriter }

func (a writerAtAdapter) WriteAt(p []byte, _ int64) (int, error) {
        return a.w.Write(p)
}

func main() {
        if len(os.Args) != 2 {
                fmt.Fprintf(os.Stderr, "Usage: %s <config.yaml>\n", os.Args[0])
                os.Exit(1)
        }

        data, _ := os.ReadFile(os.Args[1])
        var cfg Config
        if err := yaml.Unmarshal(data, &cfg); err != nil {
                log.Fatal(err)
        }

        if cfg.Threads <= 0 {
                cfg.Threads = 50
        }
        if cfg.DefaultTags == nil {
                cfg.DefaultTags = map[string]string{"public": "yes"}
        }
        if cfg.LogFile != "" {
                f, _ := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
                if f != nil {
                        log.SetOutput(f)
                }
        }
        log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
        log.SetLevel(logrus.InfoLevel)

        for _, src := range cfg.Sources {
                for _, dst := range cfg.Destinations {
                        syncPair(src, dst, &cfg)
                }
        }
}

func newClient(region, ak, sk string) (*s3.Client, *manager.Downloader, *manager.Uploader) {
        cfg, _ := config.LoadDefaultConfig(context.TODO(),
                config.WithRegion(region),
                config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
        )
        client := s3.NewFromConfig(cfg)
        return client, manager.NewDownloader(client), manager.NewUploader(client)
}

// 判断是否应该同步 + 计算目标相对路径
func shouldSync(key, srcPrefix string, mappings []MappingRule) (string, bool) {
        if srcPrefix != "" {
                if strings.HasPrefix(key, srcPrefix) {
                        return strings.TrimPrefix(key, srcPrefix), true
                }
                return "", false
        }
        for _, m := range mappings {
                if m.SourcePrefix != "" && strings.HasPrefix(key, m.SourcePrefix) {
                        return m.TargetPrefix + strings.TrimPrefix(key, m.SourcePrefix), true
                }
        }
        for _, m := range mappings {
                if m.SourcePrefix == "" {
                        return m.TargetPrefix + key, true
                }
        }
        return "", false
}

func countObjects(c *s3.Client, bucket, prefix string, mappings []MappingRule) int64 {
        var n int64
        p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
        for p.HasMorePages() {
                page, _ := p.NextPage(context.Background())
                for _, o := range page.Contents {
                        if o.Key != nil && !strings.HasSuffix(*o.Key, "/") {
                                if _, ok := shouldSync(*o.Key, prefix, mappings); ok {
                                        n++
                                }
                        }
                }
        }
        return n
}

func syncPair(src Source, dst Destination, cfg *Config) {
        srcClient, srcDL, _ := newClient(src.Region, src.AccessKeyID, src.SecretAccessKey)
        dstClient, _, dstUL := newClient(dst.Region, dst.AccessKeyID, dst.SecretAccessKey)

        total := countObjects(srcClient, src.Bucket, src.Prefix, cfg.DirectoryMappings)
        if total == 0 {
                log.Infof("无匹配对象，跳过 %s → %s", src.Name, dst.Name)
                return
        }

        p := mpb.New(mpb.WithWidth(64))
        bar := p.AddBar(total,
                mpb.PrependDecorators(
                        decor.Name(fmt.Sprintf("%s → %s ", src.Name, dst.Name)),
                        decor.CountersNoUnit("%d/%d"),
                ),
                mpb.AppendDecorators(
                        decor.Percentage(decor.WCSyncSpace),
                        decor.Name(" "),
                        // 彻底避开 UnitKB/UnitKiB 兼容性地狱 → 直接用 SizeB1000（官方推荐）
                        decor.AverageSpeed(decor.SizeB1000(0), "% .1f"),
                        decor.Name(" "),
                        decor.AverageETA(decor.ET_STYLE_GO),
                ),
        )

        sem := make(chan struct{}, cfg.Threads)
        var wg sync.WaitGroup
        ctx := context.Background()

        pager := s3.NewListObjectsV2Paginator(srcClient, &s3.ListObjectsV2Input{
                Bucket: &src.Bucket,
                Prefix: &src.Prefix,
        })

        for pager.HasMorePages() {
                page, _ := pager.NextPage(ctx)
                for _, obj := range page.Contents {
                        if obj.Key == nil || strings.HasSuffix(*obj.Key, "/") {
                                continue
                        }
                        key := *obj.Key

                        relKey, ok := shouldSync(key, src.Prefix, cfg.DirectoryMappings)
                        if !ok {
                                continue
                        }

                        targetKey := relKey
                        if dst.Prefix != "" {
                                targetKey = dst.Prefix + targetKey
                        }

                        wg.Add(1)
                        sem <- struct{}{}
                        go func(o types.Object, tKey string) {
                                defer wg.Done()
                                defer func() { <-sem }()
                                transfer(ctx, srcClient, srcDL, dstClient, dstUL, src, dst, o, tKey, cfg)
                                bar.Increment()
                        }(obj, targetKey) // 直接传值，无需 *
                }
        }
        wg.Wait()
        p.Wait()
        log.Infof("同步完成 %s → %s，共 %d 个对象", src.Name, dst.Name, total)
}

func transfer(ctx context.Context,
        srcClient *s3.Client, srcDL *manager.Downloader,
        dstClient *s3.Client, dstUL *manager.Uploader,
        src Source, dst Destination, obj types.Object, targetKey string, cfg *Config) {

        key := *obj.Key

        head, _ := dstClient.HeadObject(ctx, &s3.HeadObjectInput{
                Bucket: &dst.Bucket,
                Key:    &targetKey,
        })
        if head != nil && head.ETag != nil && obj.ETag != nil && *head.ETag == *obj.ETag {
                return
        }
        if head != nil {
                targetKey = fmt.Sprintf("%s.sync%s", targetKey, time.Now().Format("20060102T150405Z"))
                log.Warnf("冲突重命名 → %s", targetKey)
        }

        pr, pw := io.Pipe()
        go func() {
                defer pw.Close()
                _, err := srcDL.Download(ctx, writerAtAdapter{pw}, &s3.GetObjectInput{
                        Bucket: &src.Bucket,
                        Key:    &key,
                })
                if err != nil {
                        pw.CloseWithError(err)
                }
        }()

        _, err := dstUL.Upload(ctx, &s3.PutObjectInput{
                Bucket: &dst.Bucket,
                Key:    &targetKey,
                Body:   pr,
        })
        if err != nil {
                log.Errorf("上传失败 %s → %s: %v", key, targetKey, err)
                return
        }

        // 标签
        resp, _ := srcClient.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: &src.Bucket, Key: &key})
        tags := resp.TagSet
        if len(tags) == 0 {
                for k, v := range cfg.DefaultTags {
                        tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
                }
        }
        if len(tags) > 0 {
                dstClient.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
                        Bucket:  &dst.Bucket,
                        Key:     &targetKey,
                        Tagging: &types.Tagging{TagSet: tags},
                })
        }

        log.Infof("成功 %s → %s (%d bytes)", key, targetKey, obj.Size)
}