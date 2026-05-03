package cloudflare_imgbed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/errgroup"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/avast/retry-go"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

func (d *CFImgBed) Put(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) (model.Obj, error) {
	fileSize := file.GetSize()
	if fileSize >= hfDirectThreshold && d.LargeChannelType == "huggingface" {
		log.WithField("size", fileSize).Info("file exceeds threshold, using HuggingFace direct upload")
		obj, err := d.hfDirectUpload(ctx, dstDir, file, up)
		if err != nil {
			return nil, fmt.Errorf("HF direct upload failed: %w", err)
		}
		return obj, nil
	}
	obj, err := d.standardUpload(ctx, dstDir, file, up)
	if err != nil {
		return nil, fmt.Errorf("standard upload failed: %w", err)
	}
	return obj, nil
}

// standardUpload 通过普通multipart表单上传小文件
func (d *CFImgBed) standardUpload(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) (model.Obj, error) {
	fileName := file.GetName()
	fileSize := file.GetSize()
	uploadDir := getUploadDir(d, dstDir)

	channelName := d.SmallChannelName
	if fileSize >= hfDirectThreshold {
		channelName = d.LargeChannelName
	}
	if channelName == "" {
		return nil, fmt.Errorf("channel name not configured")
	}

	reader, err := getFileReader(file)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	progressReader := &progressReadCloser{ReadCloser: reader, total: fileSize, up: up}

	// 构造multipart表单
	b := &bytes.Buffer{}
	w := multipart.NewWriter(b)
	w.WriteField("uploadFolder", uploadDir)
	w.WriteField("returnFormat", "default")
	w.WriteField("channelName", channelName)
	part, err := w.CreateFormFile("file", fileName)
	if err != nil {
		return nil, err
	}
	headSize := b.Len()
	// 写入文件内容到multipart
	if _, err := io.Copy(part, progressReader); err != nil {
		return nil, err
	}
	w.Close()

	// 请求体：头部 + 已写入的文件部分 + 尾部
	bodyReader := io.MultiReader(
		bytes.NewReader(b.Bytes()[:headSize]),
		bytes.NewReader(b.Bytes()[headSize:]),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Address+UploadApi, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+d.Token)
	req.ContentLength = int64(b.Len())

	res, err := base.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upload failed %d: %s", res.StatusCode, string(body))
	}

	var resp standardUploadResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse upload response: %w", err)
	}
	if len(resp) == 0 || resp[0].Src == "" {
		return nil, fmt.Errorf("no src returned after upload")
	}

	srcPath := strings.TrimPrefix(resp[0].Src, "/file/")
	srcPath = strings.TrimPrefix(srcPath, "/")
	displayPath := stripRootPrefix(srcPath, strings.Trim(d.GetRootPath(), "/"))

	return &File{
		path:    displayPath,
		name:    fileName,
		size:    fileSize,
		modTime: file.ModTime(),
	}, nil
}

// hfDirectUpload 通过HuggingFace直传流程上传大文件
func (d *CFImgBed) hfDirectUpload(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) (model.Obj, error) {
	fileName := file.GetName()
	fileSize := file.GetSize()
	fileMime := file.GetMimetype()
	modTime := file.ModTime()
	uploadDir := getUploadDir(d, dstDir)

	// 确保文件已缓存
	if file.GetFile() == nil {
		if _, err := file.CacheFullAndWriter(nil, nil); err != nil {
			return nil, fmt.Errorf("cache file for HF upload: %w", err)
		}
	}

	sha256Hash, err := calculateSHA256(file)
	if err != nil {
		return nil, err
	}
	fileSample, err := getFileSample(file)
	if err != nil {
		return nil, err
	}

	channelName := d.LargeChannelName
	if channelName == "" {
		return nil, fmt.Errorf("LargeChannelName not configured")
	}

	reqBody := map[string]interface{}{
		"fileName":     fileName,
		"fileType":     fileMime,
		"fileSize":     fileSize,
		"sha256":       sha256Hash,
		"fileSample":   fileSample,
		"channelName":  channelName,
		"uploadFolder": uploadDir,
	}

	var getUrlResp hfGetUrlResp
	_, err = d.doRequest(http.MethodPost, HFGetUrlApi, func(req *resty.Request) {
		req.SetBody(reqBody)
		req.SetHeader("Content-Type", "application/json")
	}, &getUrlResp)
	if err != nil {
		return nil, fmt.Errorf("get HF upload URL: %w", err)
	}

	log.WithFields(log.Fields{
		"needsLfs":      getUrlResp.NeedsLfs,
		"alreadyExists": getUrlResp.AlreadyExists,
	}).Debug("HF upload URL obtained")

	if getUrlResp.AlreadyExists || !getUrlResp.NeedsLfs {
		return d.hfCommit(ctx, getUrlResp, fileName, fileSize, fileMime, modTime)
	}

	if getUrlResp.UploadAction == nil {
		return nil, fmt.Errorf("HF upload action is nil")
	}

	headers := getUrlResp.UploadAction.Header
	href := getUrlResp.UploadAction.Href

	// 重置文件读取位置
	if _, err := file.GetFile().Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek file: %w", err)
	}

	// 检查是否需要分片上传
	chunkSizeStr, needChunk := headers["chunk_size"]
	if needChunk {
		chunkSize, _ := strconv.ParseInt(chunkSizeStr, 10, 64)
		if chunkSize <= 0 {
			chunkSize = 20 * 1024 * 1024
		}
		log.WithField("chunkSize", chunkSize).Info("HF chunked upload required")

		// 提取分片URL
		partUrls := make(map[int]string)
		for k, v := range headers {
			if len(k) == 5 {
				if idx, err := strconv.Atoi(k); err == nil {
					partUrls[idx] = v
				}
			}
		}
		totalParts := len(partUrls)
		if totalParts == 0 {
			return nil, fmt.Errorf("HF chunk size specified but no part URLs found")
		}

		// 创建分段流式读取器
		ss, err := stream.NewStreamSectionReader(file, int(chunkSize), &up)
		if err != nil {
			return nil, err
		}

		// 并发分片上传，带重试
		thread := 3
		g, uploadCtx := errgroup.NewOrderedGroupWithContext(ctx, thread,
			retry.Attempts(3),
			retry.Delay(time.Second),
			retry.DelayType(retry.BackOffDelay))

		var partsMutex sync.Mutex
		parts := make([]map[string]interface{}, 0, totalParts)

		for partNumber := 1; partNumber <= totalParts; partNumber++ {
			partNumber := partNumber
			partUrl := partUrls[partNumber]
			offset := int64(partNumber-1) * chunkSize
			sizeToRead := chunkSize
			if offset+sizeToRead > fileSize {
				sizeToRead = fileSize - offset
			}

			g.GoWithLifecycle(errgroup.Lifecycle{
				Before: func(ctx context.Context) error {
					return nil
				},
				Do: func(ctx context.Context) error {
					reader, err := ss.GetSectionReader(offset, sizeToRead)
					if err != nil {
						return err
					}
					req, err := http.NewRequestWithContext(ctx, http.MethodPut, partUrl, reader)
					if err != nil {
						ss.FreeSectionReader(reader)
						return err
					}
					for key, val := range headers {
						if len(key) != 5 { // 非分片号
							req.Header.Set(key, val)
						}
					}
					req.ContentLength = sizeToRead

					res, err := base.HttpClient.Do(req)
					// 释放读取器
					ss.FreeSectionReader(reader)
					if err != nil {
						return fmt.Errorf("chunk %d upload: %w", partNumber, err)
					}
					defer res.Body.Close()

					if res.StatusCode != http.StatusOK {
						b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
						return fmt.Errorf("chunk %d failed %d: %s", partNumber, res.StatusCode, string(b))
					}

					etag := res.Header.Get("ETag")
					partsMutex.Lock()
					parts = append(parts, map[string]interface{}{
						"partNumber": partNumber,
						"etag":       etag,
					})
					partsMutex.Unlock()

					log.WithFields(log.Fields{
						"part": partNumber,
						"etag": etag,
					}).Debug("HF chunk uploaded")
					return nil
				},
				After: func(err error) {
					// 读取器已在Do中释放
				},
			})
			// 检查上下文是否已取消
			if utils.IsCanceled(uploadCtx) {
				break
			}
		}

		if err := g.Wait(); err != nil {
			return nil, fmt.Errorf("HF chunked upload: %w", err)
		}

		// 合并分片
		sort.Slice(parts, func(i, j int) bool {
			return parts[i]["partNumber"].(int) < parts[j]["partNumber"].(int)
		})

		mergeBody := map[string]interface{}{"oid": getUrlResp.Oid, "parts": parts}
		mergeJson, _ := json.Marshal(mergeBody)
		mergeReq, err := http.NewRequestWithContext(ctx, http.MethodPost, href, bytes.NewReader(mergeJson))
		if err != nil {
			return nil, fmt.Errorf("create merge request: %w", err)
		}
		mergeReq.Header.Set("Content-Type", "application/vnd.git-lfs+json")
		// 携带原始非分片头
		for key, val := range headers {
			if key != "chunk_size" && len(key) != 5 {
				mergeReq.Header.Set(key, val)
			}
		}

		res, err := base.HttpClient.Do(mergeReq)
		if err != nil {
			return nil, fmt.Errorf("merge chunks: %w", err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
			return nil, fmt.Errorf("merge failed %d: %s", res.StatusCode, string(b))
		}
		io.Copy(io.Discard, res.Body)
		log.Info("HF chunks merged successfully")

	} else {
		// 单文件直接上传
		cachedFile := file.GetFile()
		if _, err := cachedFile.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		progressReader := &progressReadCloser{
			ReadCloser: io.NopCloser(cachedFile),
			total:      fileSize,
			up:         up,
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, href, progressReader)
		if err != nil {
			return nil, err
		}
		req.ContentLength = fileSize
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		res, err := base.HttpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("HF direct upload: %w", err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
			return nil, fmt.Errorf("HF direct upload failed %d: %s", res.StatusCode, string(b))
		}
		io.Copy(io.Discard, res.Body)
		log.Info("HF direct single upload completed")
	}

	return d.hfCommit(ctx, getUrlResp, fileName, fileSize, fileMime, modTime)
}

func (d *CFImgBed) hfCommit(ctx context.Context, getUrlResp hfGetUrlResp, fileName string, fileSize int64, fileMime string, modTime time.Time) (model.Obj, error) {
	commitBody := map[string]interface{}{
		"fullId":      getUrlResp.FullID,
		"filePath":    getUrlResp.FilePath,
		"sha256":      getUrlResp.Oid,
		"fileSize":    fileSize,
		"fileName":    fileName,
		"fileType":    fileMime,
		"channelName": getUrlResp.ChannelName,
	}
	var commitResp hfCommitResp
	_, err := d.doRequest(http.MethodPost, HFCommitApi, func(req *resty.Request) {
		req.SetBody(commitBody)
		req.SetHeader("Content-Type", "application/json")
	}, &commitResp)
	if err != nil {
		return nil, fmt.Errorf("HF commit: %w", err)
	}
	if !commitResp.Success || commitResp.Src == "" {
		return nil, fmt.Errorf("HF commit failed")
	}

	srcPath := strings.TrimPrefix(commitResp.Src, "/file/")
	srcPath = strings.TrimPrefix(srcPath, "/")
	displayPath := stripRootPrefix(srcPath, strings.Trim(d.GetRootPath(), "/"))

	return &File{
		path:    displayPath,
		name:    fileName,
		size:    fileSize,
		modTime: modTime,
	}, nil
}

func getFileReader(file model.FileStreamer) (io.ReadCloser, error) {
	if cached := file.GetFile(); cached != nil {
		if _, err := cached.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek cached file: %w", err)
		}
		if rc, ok := cached.(io.ReadCloser); ok {
			return rc, nil
		}
		return io.NopCloser(cached), nil
	}
	if rc, ok := file.(io.ReadCloser); ok {
		return rc, nil
	}
	return io.NopCloser(file), nil
}

type progressReadCloser struct {
	io.ReadCloser
	total int64
	read  int64
	up    driver.UpdateProgress
}

func (r *progressReadCloser) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.read += int64(n)
	if r.total > 0 && r.up != nil {
		r.up(100 * float64(r.read) / float64(r.total))
	}
	return
}