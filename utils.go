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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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
	if err != nil {
		return false
	}
	return len(resp.Contents) > 0 || len(resp.CommonPrefixes) > 0
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