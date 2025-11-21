// utils.go
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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type writerAtAdapter struct{ w *io.PipeWriter }

func (a writerAtAdapter) WriteAt(p []byte, _ int64) (int, error) {
	return a.w.Write(p)
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

func dirExistsS3(client *s3.Client, bucket, prefix string) bool {
	resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:  &bucket,
		Prefix:  &prefix,
		MaxKeys: aws.Int32(1),
	})
	return err == nil && (len(resp.Contents) > 0 || len(resp.CommonPrefixes) > 0)
}

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

func syncOneDirectory(srcClient *s3.Client, srcDL *manager.Downloader, dstClient *s3.Client, dstUL *manager.Uploader,
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

			head, _ := dstClient.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: &dstBucket,
				Key:    &targetKey,
			})
			if head != nil && head.ETag != nil && obj.ETag != nil && *head.ETag == *obj.ETag {
				continue
			}

			if head != nil {
				targetKey = fmt.Sprintf("%s.sync%s", prefix+rel, time.Now().Format("20060102T150405Z"))
				log.Warnf("冲突重命名: %s → %s", key, targetKey)
			}

			pr, pw := io.Pipe()
			go func(srcKey string) {
				defer pw.Close()
				_, err := srcDL.Download(context.Background(), writerAtAdapter{pw}, &s3.GetObjectInput{
					Bucket: &srcBucket,
					Key:    &srcKey,
				})
				if err != nil {
					pw.CloseWithError(err)
				}
			}(key)

			_, err = dstUL.Upload(context.Background(), &s3.PutObjectInput{
				Bucket: &dstBucket,
				Key:    &targetKey,
				Body:   pr,
			})
			if err != nil {
				fail++
				continue
			}
			success++

			// 复制标签
			tagResp, _ := srcClient.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{
				Bucket: &srcBucket,
				Key:    &key,
			})
			if len(tagResp.TagSet) > 0 {
				dstClient.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
					Bucket:  &dstBucket,
					Key:     &targetKey,
					Tagging: &types.Tagging{TagSet: tagResp.TagSet},
				})
			}

			// 复制 ACL
			aclResp, _ := srcClient.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{
				Bucket: &srcBucket,
				Key:    &key,
			})
			if len(aclResp.Grants) > 0 {
				var grants []types.Grant
				for _, g := range aclResp.Grants {
					if g.Grantee != nil {
						grants = append(grants, types.Grant{
							Grantee:    g.Grantee,
							Permission: g.Permission,
						})
					}
				}
				dstClient.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
					Bucket: &dstBucket,
					Key:    &targetKey,
					AccessControlPolicy: &types.AccessControlPolicy{
						Grants: grants,
						Owner:  aclResp.Owner,
					},
				})
			}
		}
	}
	return success, fail
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
		fpath := filepath.Join(dest, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, 0755)
			continue
		}
		os.MkdirAll(filepath.Dir(fpath), 0755)
		outFile, _ := os.Create(fpath)
		rc, _ := f.Open()
		io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
	}
	return nil
}

func cleanupFiles(zipPath, dir string) {
	os.Remove(zipPath)
	os.RemoveAll(dir)
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