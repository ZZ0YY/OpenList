package cloudflare_imgbed

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

const (
	ListApi            = "/api/manage/list"
	UploadApi          = "/upload"
	HFGetUrlApi        = "/upload/huggingface/getUploadUrl"
	HFCommitApi        = "/upload/huggingface/commitUpload"
	hfDirectThreshold  int64 = 20 * 1024 * 1024
	fileSampleSize     = 512
)

// doRequest 是所有API请求的统一入口，包含重试、错误解析和日志记录。
func (d *CFImgBed) doRequest(method, urlPath string, callback func(*resty.Request), resp interface{}) ([]byte, error) {
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		req := d.client.R()
		if callback != nil {
			callback(req)
		}
		if resp != nil {
			req.SetResult(resp)
		}

		res, err := req.Execute(method, urlPath)
		if err != nil {
			log.WithError(err).Warnf("request %s %s failed, attempt %d/%d", method, urlPath, i+1, maxRetries)
			if i < maxRetries-1 {
				time.Sleep(time.Duration(i+1) * time.Second)
				continue
			}
			return nil, err
		}

		body := res.Body()
		var apiErr apiError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			if apiErr.Error != "" || apiErr.Message != "" {
				msg := apiErr.Error
				if msg == "" {
					msg = apiErr.Message
				}
				return nil, fmt.Errorf("API error: %s", msg)
			}
		}

		if res.StatusCode() == 429 {
			sleep := time.Duration(i+1) * 2 * time.Second
			log.Warnf("rate limited on %s %s, retrying in %v", method, urlPath, sleep)
			time.Sleep(sleep)
			continue
		}

		if res.IsError() {
			return nil, fmt.Errorf("HTTP %d on %s %s", res.StatusCode(), method, urlPath)
		}
		return body, nil
	}
	return nil, fmt.Errorf("max retries exceeded for %s %s", method, urlPath)
}

// 【新增】合并前置数据准备：优先复用底层哈希，降低读取 I/O，并一并获取 fileSample
func prepareHFUploadData(file model.FileStreamer) (string, string, error) {
	// HF 直传和分片需要 Seek，因此必须要缓存（且因为只调用一次，放这里很安全）
	if file.GetFile() == nil {
		if _, err := file.CacheFullAndWriter(nil, nil); err != nil {
			return "", "", fmt.Errorf("cache file for HF upload: %w", err)
		}
	}

	cached := file.GetFile()

	// 1. 获取 SHA256：优先尝试从 OpenList 底层缓存对象中提取
	sha256Hex := file.GetHash().GetHash(utils.SHA256)
	if len(sha256Hex) == 0 {
		log.Debug("SHA256 not found in HashInfo, calculating from cached file")
		if _, err := cached.Seek(0, io.SeekStart); err != nil {
			return "", "", fmt.Errorf("seek file for sha256: %w", err)
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, cached); err != nil {
			return "", "", fmt.Errorf("calculate SHA256: %w", err)
		}
		sha256Hex = hex.EncodeToString(hash.Sum(nil))
	}

	// 2. 获取 fileSample：仅读取前 512 字节
	if _, err := cached.Seek(0, io.SeekStart); err != nil {
		return "", "", fmt.Errorf("seek file for sample: %w", err)
	}
	sampleBuf := make([]byte, fileSampleSize)
	n, err := io.ReadFull(cached, sampleBuf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return "", "", fmt.Errorf("read file sample: %w", err)
	}
	sampleBase64 := base64.StdEncoding.EncodeToString(sampleBuf[:n])

	return sha256Hex, sampleBase64, nil
}

func getUploadDir(d *CFImgBed, dstDir model.Obj) string {
	rootPath := strings.Trim(d.GetRootPath(), "/")
	var dirPath string
	if dstDir != nil {
		dirPath = strings.Trim(dstDir.GetPath(), "/")
	}
	if rootPath != "" && dirPath != "" {
		return path.Join(rootPath, dirPath)
	}
	if rootPath != "" {
		return rootPath
	}
	return dirPath
}

func stripRootPrefix(p, rootPath string) string {
	if rootPath == "" {
		return p
	}
	prefix := rootPath + "/"
	if strings.HasPrefix(p, prefix) {
		return strings.TrimPrefix(p, prefix)
	}
	return p
}

// 辅助函数：安全转义 MIME Header 中的特殊字符
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")
func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}