package cloudflare_imgbed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
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

// standardUpload 通过普通multipart表单上传文件（【已优化】采用零内存占用流式拼接构建）
func (d *CFImgBed) standardUpload(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) (model.Obj, error) {
	fileName := file.GetName()
	fileSize := file.GetSize()
	fileMime := file.GetMimetype()
	uploadDir := getUploadDir(d, dstDir)

	channelName := d.SmallChannelName
	if fileSize >= hfDirectThreshold {
		channelName = d.LargeChannelName
		// 提醒：对于非 HF 渠道但大体积的文件，记录警告日志
		log.WithField("size", fileSize).Warnf("File exceeds %d bytes threshold but non-HF channel is used. Upload might fail due to server limits.", hfDirectThreshold)
	}
	if channelName == "" {
		return nil, fmt.Errorf("channel name not configured")
	}

	// 1. 将非文件的元数据参数转移至 URL Query（遵循 API 规范且避免塞入 FormData 增加处理复杂度和内存开销）
	reqUrl, _ := url.Parse(strings.TrimRight(d.Address, "/") + UploadApi)
	q := reqUrl.Query()
	if uploadDir != "" {
		q.Set("uploadFolder", uploadDir)
	}
	q.Set("returnFormat", "default")
	q.Set("channelName", channelName)
	reqUrl.RawQuery = q.Encode()

	// 2. 手动构建零拷贝的 multipart/form-data 头尾
	var headBuf bytes.Buffer
	w := multipart.NewWriter(&headBuf)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, escapeQuotes(fileName)))
	if fileMime == "" {
		fileMime = "application/octet-stream"
	}
	h.Set("Content-Type", fileMime)
	if _, err := w.CreatePart(h); err != nil {
		return nil, fmt.Errorf("create multipart part: %w", err)
	}
	boundary := w.Boundary()
	// 构建固定的表单闭合标识符
	tailStr := fmt.Sprintf("\r\n--%s--\r\n", boundary)

	reader, err := getFileReader(file)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 挂载上传进度监控
	progressReader := &progressReadCloser{ReadCloser: reader, total: fileSize, up: up}

	// 3. 利用 io.MultiReader 将 "头部Buffer" + "文件流" + "尾部字符串" 无缝拼接为虚拟的连续流，0 额外内存消耗
	bodyStream := io.MultiReader(
		bytes.NewReader(headBuf.Bytes()),
		progressReader,
		strings.NewReader(tailStr),
	)

	// 【新增】接入 OpenList 统一全局限速器
	rateLimitedReader := driver.NewLimitedUploadStream(ctx, bodyStream)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqUrl.String(), rateLimitedReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+d.Token)
	
	// 核心：精确指明请求体总长度，避免 Cloudflare 节点拒绝 Chunked 分块传输（Transfer-Encoding）
	req.ContentLength = int64(headBuf.Len()) + fileSize + int64(len(tailStr))

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

	// 【优化】复用底层哈希，优化全量文件扫描时间
	sha256Hash, fileSample, err := prepareHFUploadData(file)
	if err != nil {
		return nil, fmt.Errorf("prepare HF upload data: %w", err)
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

	// 重置文件读取位置（由于 prepareHFUploadData 已经确保存储为 cached 且能 Seek）
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

		// 创建分段流式读取器，这里不再传递 up，将由网络请求完成后来接管真实的进度汇报，防止假死！
		ss, err := stream.NewStreamSectionReader(file, int(chunkSize), nil)
		if err != nil {
			return nil, err
		}

		// 【优化】使用动态自定义上传线程数
		thread := d.UploadThread
		if thread <= 0 {
			thread = 3
		}
		if thread > totalParts {
			thread = totalParts
		}

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
					
					// 【新增】接入全局限速器
					limitedReader := driver.NewLimitedUploadStream(ctx, reader)
					
					req, err := http.NewRequestWithContext(ctx, http.MethodPut, partUrl, limitedReader)
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

					// 【优化】依靠真实的 HTTP 请求完成次数汇报进度（与 123_open 一致的做法）
					if up != nil {
						progress := 100 * float64(g.Success()+1) / float64(totalParts)
						up(progress)
					}

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
		
		// 【新增】接入全局限速器
		limitedReader := driver.NewLimitedUploadStream(ctx, progressReader)
		
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, href, limitedReader)
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

func (r *progressReadCloser) Read(p[]byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.read += int64(n)
	if r.total > 0 && r.up != nil {
		r.up(100 * float64(r.read) / float64(r.total))
	}
	return
}