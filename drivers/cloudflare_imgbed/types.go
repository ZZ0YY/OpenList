package cloudflare_imgbed

import (
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// ============================================================
// model.Obj 实现 — File / Dir
// ============================================================

type File struct {
	path    string
	name    string
	size    int64
	modTime time.Time
	mime    string
}

func (f *File) GetPath() string     { return f.path }
func (f *File) GetName() string     { return f.name }
func (f *File) ModTime() time.Time  { return f.modTime }
func (f *File) CreateTime() time.Time { return f.modTime }
func (f *File) GetSize() int64      { return f.size }
func (f *File) IsDir() bool         { return false }
func (f *File) GetID() string       { return f.path }
func (f *File) GetHash() utils.HashInfo { return utils.HashInfo{} }

type Dir struct {
	path string
	name string
}

func (d *Dir) GetPath() string     { return d.path }
func (d *Dir) GetName() string     { return d.name }
func (d *Dir) ModTime() time.Time  { return time.Time{} }
func (d *Dir) CreateTime() time.Time { return time.Time{} }
func (d *Dir) GetSize() int64      { return 0 }
func (d *Dir) IsDir() bool         { return true }
func (d *Dir) GetID() string       { return d.path }
func (d *Dir) GetHash() utils.HashInfo { return utils.HashInfo{} }

var _ model.Obj = (*File)(nil)
var _ model.Obj = (*Dir)(nil)

// ============================================================
// 列表 API 响应结构体
// ============================================================

const listPageSize = 1000

type ListResponse struct {
	Files       []FileItem `json:"files"`
	Directories []string   `json:"directories"`
}

type FileItem struct {
	Name     string                 `json:"name"`
	Metadata map[string]interface{} `json:"metadata"`
}

// ============================================================
// 上传 API 响应结构体
// ============================================================

type apiError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type standardUploadResp []struct {
	Src string `json:"src"`
}

type hfGetUrlResp struct {
	Success       bool          `json:"success"`
	FullID        string        `json:"fullId"`
	FilePath      string        `json:"filePath"`
	ChannelName   string        `json:"channelName"`
	Repo          string        `json:"repo"`
	NeedsLfs      bool          `json:"needsLfs"`
	AlreadyExists bool          `json:"alreadyExists"`
	Oid           string        `json:"oid"`
	UploadAction  *UploadAction `json:"uploadAction"`
}

type UploadAction struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header"`
}

type hfCommitResp struct {
	Success bool   `json:"success"`
	Src     string `json:"src"`
	FileUrl string `json:"fileUrl"`
	FullID  string `json:"fullId"`
}

// ============================================================
// 元数据辅助工具函数
// ============================================================

func getString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch val := v.(type) {
			case string:
				return val
			case float64:
				return strconv.FormatInt(int64(val), 10)
			default:
				return fmt.Sprintf("%v", val)
			}
		}
	}
	return ""
}

func getInt64(m map[string]interface{}, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch val := v.(type) {
			case string:
				n, _ := strconv.ParseInt(val, 10, 64)
				return n
			case float64:
				return int64(val)
			case int64:
				return val
			}
		}
	}
	return 0
}

func parseFile(item FileItem) *File {
	name := path.Base(item.Name)
	var size int64
	var modTime time.Time
	var mime string

	if item.Metadata != nil {
		size = getInt64(item.Metadata, "FileSizeBytes", "File-Size")
		mime = getString(item.Metadata, "FileType", "File-Mime")
		ts := getInt64(item.Metadata, "TimeStamp")
		if ts > 0 {
			modTime = time.UnixMilli(ts)
		}
	}

	return &File{
		path:    item.Name,
		name:    name,
		size:    size,
		modTime: modTime,
		mime:    mime,
	}
}

func parseDir(dirPath string) *Dir {
	return &Dir{
		path: dirPath,
		name: path.Base(dirPath),
	}
}
