package main

import (
	conf_tool "Tool-Library/components/conf-tool"
	"Tool-Library/components/filemode"
	"Tool-Library/components/md5"
	"bytes"
	"flag"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
)

var httpAddr = flag.String("httpAddr", "http://110.40.227.205:7787/client/cs/", "获取CS http地址")

var confPath = flag.String("confPath", "./conf/", "配置表路径")

var csPath = flag.String("csPath", "./gen/get-cs/cs/", "cs表路径")

type response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`

	// code == 200时，返回的数据
	Data *ConfigCsJson `json:"data"`
}

type ConfigCsJson struct {
	FileMd5 string `json:"file_md5"`

	FileSize int64 `json:"file_size"`

	CsBody []byte `json:"cs_body"`
}

func main() {
	flag.Parse()

	// 遍历conf文件夹
	data, err := conf_tool.ZipXlsxFiles(*confPath)
	if err != nil {
		logrus.Error("遍历文件夹生成Data失败：", err)
		return
	}

	body, err := doHttpGet(*httpAddr, data)

	if err != nil {
		logrus.Error("http请求失败：", err)
		return
	}

	resp := &response{}
	err = jsoniter.Unmarshal(body, resp)
	if err != nil {
		logrus.Errorf("解析json失败：%s", err)
		return
	}

	fmt.Println(resp.Data.FileMd5, resp.Data.FileSize)

	fileMap, err := conf_tool.UnpackData(resp.Data.CsBody)
	if err != nil {
		logrus.Errorf("解压缩失败：%s", err)
	}

	err = filemode.MkdirAll(*csPath, 777)

	if err != nil {
		logrus.Errorf("创建get-cs文件夹失败：%s", err)
	}

	for filename, fileBytes := range fileMap {
		// 将excel写入到本地磁盘
		excelPath := *csPath + filename
		if err = conf_tool.WriteFile(excelPath, fileBytes); err != nil {
			fmt.Println("写入excel失败：", err, excelPath)
			return
		}
	}
}

func doHttpGet(httpAddr string, data []byte) ([]byte, error) {
	md5String := md5.String(data)
	size := len(data)

	// 发送到服务器进行解析
	r, err := http.NewRequest(http.MethodGet, httpAddr, bytes.NewReader(data))
	if err != nil {
		return nil, errors.Wrapf(err, "new request fail")
	}

	r.Header.Set("file_md5", md5String)
	r.Header.Set("file_size", fmt.Sprintf("%d", size))

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, errors.Wrapf(err, "do request fail")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "read response fail")
	}

	logrus.WithField("code", resp.StatusCode).Debug(string(body))

	return body, nil
}
