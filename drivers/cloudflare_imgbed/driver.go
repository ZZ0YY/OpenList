package cloudflare_imgbed

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

type CFImgBed struct {
	model.Storage
	Addition
	client *resty.Client
}

func (d *CFImgBed) Config() driver.Config         { return config }
func (d *CFImgBed) GetAddition() driver.Additional { return &d.Addition }

func (d *CFImgBed) Init(ctx context.Context) error {
	// 【新增】初始化并校验上传并发数，确保不越界
	if d.UploadThread <= 0 || d.UploadThread > 32 {
		d.UploadThread = 3
	}

	d.client = resty.New().
		SetBaseURL(strings.TrimRight(d.Address, "/")).
		SetHeader("Authorization", "Bearer "+d.Token).
		SetDebug(false)

	// 轻量级连通性校验：尝试列出根目录，验证token和地址有效性
	_, err := d.doRequest(http.MethodGet, ListApi, func(req *resty.Request) {
		req.SetQueryParams(map[string]string{
			"start": "0",
			"count": "1",
			"dir":   "/",
		})
	}, nil)
	if err != nil {
		return fmt.Errorf("init verification failed: %w", err)
	}
	log.Info("Cloudflare ImgBed driver initialized successfully")
	return nil
}

func (d *CFImgBed) Drop(ctx context.Context) error { return nil }

func buildReqPath(rootPath, dirPath string) string {
	rootPath = strings.Trim(rootPath, "/")
	dirPath = strings.Trim(dirPath, "/")
	if dirPath == "" || dirPath == rootPath {
		return rootPath
	}
	if rootPath == "" {
		return dirPath
	}
	return rootPath + "/" + dirPath
}

func (d *CFImgBed) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	rootPath := strings.Trim(d.GetRootPath(), "/")
	var dirPath string
	if dir != nil {
		dirPath = strings.Trim(dir.GetPath(), "/")
	}
	reqPath := buildReqPath(rootPath, dirPath)

	dirSeen := make(map[string]bool)
	fileSeen := make(map[string]bool)
	objs := make([]model.Obj, 0)

	start := 0
	for {
		var resp ListResponse
		_, err := d.doRequest(http.MethodGet, ListApi, func(req *resty.Request) {
			req.SetQueryParams(map[string]string{
				"dir":   reqPath,
				"start": fmt.Sprintf("%d", start),
				"count": fmt.Sprintf("%d", listPageSize),
			})
		}, &resp)
		if err != nil {
			return nil, err
		}

		for _, rawDir := range resp.Directories {
			cleanDir := strings.TrimRight(rawDir, "/")
			p := stripRootPrefix(cleanDir, rootPath)
			if !dirSeen[p] {
				dirSeen[p] = true
				objs = append(objs, parseDir(p))
			}
		}

		for _, item := range resp.Files {
			p := stripRootPrefix(item.Name, rootPath)
			if !fileSeen[p] {
				fileSeen[p] = true
				objs = append(objs, parseFile(FileItem{Name: p, Metadata: item.Metadata}))
			}
		}

		if len(resp.Files)+len(resp.Directories) < listPageSize {
			break
		}
		start += listPageSize
	}
	return objs, nil
}

func (d *CFImgBed) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	rootPath := strings.Trim(d.GetRootPath(), "/")
	filePath := strings.Trim(file.GetPath(), "/")

	var fullPath string
	if rootPath != "" && filePath != "" {
		fullPath = rootPath + "/" + filePath
	} else if rootPath != "" {
		fullPath = rootPath
	} else {
		fullPath = filePath
	}

	link := strings.TrimRight(d.Address, "/") + "/file/" + utils.EncodePath(fullPath)
	return &model.Link{URL: link}, nil
}

func (d *CFImgBed) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) (model.Obj, error) {
	// 获取父路径
	var parentPath string
	if parentDir != nil {
		parentPath = parentDir.GetPath()
	}
	// 拼接新目录的完整路径
	fullPath := path.Join(parentPath, dirName)
	
	log.Debugf("MakeDir (virtual): %s", fullPath)

	// 直接返回一个虚拟的目录对象。
	// 这样 AList/OpenList 会认为目录已经“创建”好了，
	// 接着就会调用 Put 接口去上传文件。
	return &Dir{
		path: fullPath,
		name: dirName,
	}, nil
}
// 以下接口图床 API 暂不支持


func (d *CFImgBed) Move(ctx context.Context, srcObj, dstDir model.Obj) (model.Obj, error) {
	return nil, errs.NotImplement
}
func (d *CFImgBed) Rename(ctx context.Context, srcObj model.Obj, newName string) (model.Obj, error) {
	return nil, errs.NotImplement
}
func (d *CFImgBed) Copy(ctx context.Context, srcObj, dstDir model.Obj) (model.Obj, error) {
	return nil, errs.NotImplement
}
func (d *CFImgBed) Remove(ctx context.Context, obj model.Obj) error { return errs.NotImplement }
func (d *CFImgBed) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	return nil, errs.NotImplement
}

var _ driver.Driver = (*CFImgBed)(nil)