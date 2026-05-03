package cloudflare_imgbed

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	driver.RootPath
	Address          string `json:"address" type:"text" required:"true" default:"" help:"API domain, e.g. https://img.example.com"`
	Token            string `json:"token" type:"text" required:"true" default:"" help:"API authentication token"`
	SmallChannelName string `json:"smallChannelName" type:"text" required:"false" default:"" help:"Upload channel name for files smaller than 20MB"`
	LargeChannelName string `json:"largeChannelName" type:"text" required:"false" default:"" help:"Upload channel name for files larger than or equal to 20MB"`
	LargeChannelType string `json:"largeChannelType" type:"select" required:"false" default:"" options:",huggingface" help:"Upload channel type for large files, e.g. huggingface"`
}

var config = driver.Config{
	Name:              "cloudflare_imgbed",
	LocalSort:         false,
	OnlyProxy:         false,
	NoCache:           false,
	NoUpload:          false,
	NeedMs:            false,
	DefaultRoot:       "/",
	CheckStatus:       false,
	Alert:             "",
	NoOverwriteUpload: false,
	NoLinkURL:         false,
}

func init() {
	op.RegisterDriver(func() driver.Driver { return &CFImgBed{} })
}
