package main

import (
	conf_tool "Tool-Library/components/conf-tool"
	"Tool-Library/components/filemode"
	"Tool-Library/components/md5"
	"flag"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"gocloud.dev/blob"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// 在远程服务器上运行一个命令行，将表格转换出来

const packVersion = 3 // 打包版本号，如果发生变化，需要重新打包

var port = flag.Int("port", 7787, "listen port")

var dbBasePath = "./gen/conf-http/"

var dbPath = "./gen/conf-http/" + strconv.Itoa(packVersion) + "/"

func main() {
	flag.Parse()

	m := manager{}

	filemode.MkdirAll("./tmp/", 0777)

	http.HandleFunc("/server/", m.handleFunc)

	http.Handle("/client/sqlite/", http.StripPrefix("/client/sqlite/", http.FileServer(http.Dir(dbBasePath))))

	http.HandleFunc("/client/cs/", clientCsHandleFunc)

	fmt.Println("listen port:", *port)
	http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
}

type response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`

	// code == 200时，返回的数据
	Data interface{} `json:"data"`
}

func writeErrMsg(w http.ResponseWriter, s string) {
	jsoniter.NewEncoder(w).Encode(&response{
		Code:    400,
		Message: s,
	})

	//return []byte(fmt.Sprintf(`{"code":400,"message":"%s"}`, s))
}

func writeConfigJson(w http.ResponseWriter, data *ConfigJson, cmdOutPut string) {
	jsoniter.NewEncoder(w).Encode(&response{
		Code:    200,
		Data:    data,
		Message: cmdOutPut,
	})
}

type manager struct {
	bucket *blob.Bucket
}

func (m *manager) handleFunc(w http.ResponseWriter, r *http.Request) {
	// 从header中获取md5值
	fileMd5 := r.Header.Get("file_md5")
	fileSize := r.Header.Get("file_size")

	if fileMd5 == "" || fileSize == "" {
		fmt.Println("md5 or size is empty")
		writeErrMsg(w, "md5 or size is empty")
		return
	}

	var fileSizeInt64 int64
	val, err := strconv.ParseInt(fileSize, 10, 64)
	if err != nil || val <= 0 {
		fmt.Println("invalid file size", fileSize, "  val :", val)
		writeErrMsg(w, "invalid size")
		return
	}
	fileSizeInt64 = val

	confJson := &ConfigJson{
		FileMd5:  fileMd5,
		FileSize: fileSizeInt64,
	}

	//检查对应的版本文件是否存在
	versionName := getVersionName(fileMd5, fileSizeInt64)

	verionData, err := os.ReadFile(dbPath + versionName)

	if !os.IsNotExist(err) {
		fmt.Println("存在对应版本:", dbPath+versionName)
		confJson.VersionPath = strconv.Itoa(packVersion) + "/" + versionName
		confJson.VersionMd5 = md5.String(verionData)

		writeConfigJson(w, confJson, "使用已存在对应版本:"+dbPath+versionName)
		return
	}

	//重新生成
	data, err := io.ReadAll(r.Body)

	dataMd5 := md5.String(data)
	dataSize := int64(len(data))

	if fileMd5 != dataMd5 || fileSizeInt64 != dataSize {
		writeErrMsg(w, fmt.Sprintf("header和body的数据不一致, headerMd5: %v, bodyMd5: %v, headerSize: %v, bodySize: %v", fileMd5, dataMd5, fileSizeInt64, dataSize))
		return
	}

	err = Generate(m.bucket, confJson, data)

	if err != nil {
		fmt.Println("重新生成版本文件报错,generate:", err)
		writeErrMsg(w, fmt.Sprintf("生成错误，err: %v", err))
		return
	}

	writeConfigJson(w, confJson, "")
	return
}

// data是个zip压缩文件
func Generate(bucket *blob.Bucket, configJson *ConfigJson, packBytes []byte) error {
	genMux := &sync.Mutex{}
	genMux.Lock()
	defer genMux.Unlock()

	tempDir, err := os.MkdirTemp("tmp/", "excel-")

	if err != nil {
		return errors.Errorf("os.MkdirTemp fail, err: %v", err)
	}

	defer os.RemoveAll(tempDir)

	err = filemode.MkdirAll(dbPath, 777)

	if err != nil {
		return errors.Errorf("filemode.MkdirAll(%s) fail, err: %v", dbPath, err)
	}

	fileMap, err := conf_tool.UnpackData(packBytes)
	if err != nil {
		return errors.Wrapf(err, "unpackData")
	}

	for filename, fileBytes := range fileMap {
		if strings.Contains(filename, "__MACOSX") {
			continue
		}

		basename := filepath.Base(filename)

		if strings.HasPrefix(basename, "~$") {
			continue
		}

		if !strings.HasSuffix(basename, ".xlsx") {
			continue
		}

		// 将excel写入到本地磁盘
		excelPath := fmt.Sprintf("%s/%s", tempDir, filename)
		if err = conf_tool.WriteFile(excelPath, fileBytes); err != nil {
			return errors.Wrapf(err, "writeFile")
		}
	}

	errorRun := conf_tool.RunCommand("./bin/exceltodb.exe", "--dbPath="+dbPath, "--conf="+tempDir, "--joinPath="+strconv.Itoa(packVersion)+"/")

	if errorRun != nil {
		return errors.Errorf("EXE 执行失败:%v", errorRun)
	}

	_, errVersionStat := os.Stat(dbPath + "version.txt")

	if os.IsNotExist(errVersionStat) {
		return errors.Errorf("不存在版本文件")
	}

	dataMd5 := md5.String(packBytes)
	dataSize := int64(len(packBytes))

	os.Rename(dbPath+"version.txt", dbPath+getVersionName(dataMd5, dataSize))

	configJson.VersionPath = strconv.Itoa(packVersion) + "/" + getVersionName(dataMd5, dataSize)

	fmt.Println("生成成功:", configJson.VersionPath)

	return nil
}

type ConfigJson struct {
	FileMd5 string `json:"file_md5"`

	FileSize int64 `json:"file_size"`

	VersionPath string `json:"version_path"`

	VersionMd5 string `json:"version_md5"`
}

type ConfigCsJson struct {
	FileMd5 string `json:"file_md5"`

	FileSize int64 `json:"file_size"`

	CsBody []byte `json:"cs_body"`
}

func getVersionName(fileMd5 string, fileSizeInt64 int64) string {
	return fmt.Sprintf("version_%s_%d.txt", fileMd5, fileSizeInt64)
}

func clientCsHandleFunc(w http.ResponseWriter, r *http.Request) {

	fmt.Println("------开始生成前端CS:clientCsHandleFunc------")

	// 从header中获取md5值
	fileMd5 := r.Header.Get("file_md5")
	fileSize := r.Header.Get("file_size")

	if fileMd5 == "" || fileSize == "" {
		fmt.Println("md5 or size is empty")
		writeErrMsg(w, "md5 or size is empty")
		return
	}

	var fileSizeInt64 int64
	val, err := strconv.ParseInt(fileSize, 10, 64)
	if err != nil || val <= 0 {
		fmt.Println("invalid file size", fileSize, "  val :", val)
		writeErrMsg(w, "invalid size")
		return
	}
	fileSizeInt64 = val

	data, err := io.ReadAll(r.Body)

	dataMd5 := md5.String(data)
	dataSize := int64(len(data))

	if fileMd5 != dataMd5 || fileSizeInt64 != dataSize {
		writeErrMsg(w, fmt.Sprintf("header和body的数据不一致, headerMd5: %v, bodyMd5: %v, headerSize: %v, bodySize: %v", fileMd5, dataMd5, fileSizeInt64, dataSize))
		return
	}

	confcsJson := &ConfigCsJson{
		FileMd5:  fileMd5,
		FileSize: fileSizeInt64,
	}

	err = GenerateCs(confcsJson, data)

	jsoniter.NewEncoder(w).Encode(&response{
		Code:    200,
		Data:    confcsJson,
		Message: fmt.Sprintf("Err:%v", err),
	})

	fmt.Println("------生成前端CS:clientCsHandleFunc结束------")

	return
}

// data是个zip压缩文件
func GenerateCs(configJson *ConfigCsJson, packBytes []byte) error {
	genMux := &sync.Mutex{}
	genMux.Lock()
	defer genMux.Unlock()

	errCreate := filemode.MkdirAll("./gen/", 777)

	if errCreate != nil {
		return errors.Errorf("filemode.MkdirAll(./gen/) fail, err: %v", errCreate)
	}

	tempConfDir, err := os.MkdirTemp("tmp/", "excel-")

	if err != nil {
		return errors.Errorf("filemode.MkdirAll(%s) fail, err: %v", tempConfDir, err)
	}

	tempCsDir, err := os.MkdirTemp("tmp/", "cs-")

	if err != nil {
		return errors.Errorf("filemode.MkdirAll(%s) fail, err: %v", tempCsDir, err)
	}

	defer os.RemoveAll(tempConfDir)

	defer os.RemoveAll(tempCsDir)

	if err != nil {
		return errors.Errorf("filemode.MkdirAll(%s) fail, err: %v", dbPath, err)
	}

	fileMap, err := conf_tool.UnpackData(packBytes)
	if err != nil {
		return errors.Wrapf(err, "unpackData")
	}

	for filename, fileBytes := range fileMap {
		if strings.Contains(filename, "__MACOSX") {
			continue
		}

		basename := filepath.Base(filename)

		if strings.HasPrefix(basename, "~$") {
			continue
		}

		if !strings.HasSuffix(basename, ".xlsx") {
			continue
		}

		// 将excel写入到本地磁盘
		excelPath := fmt.Sprintf("%s/%s", tempConfDir, filename)
		if err = conf_tool.WriteFile(excelPath, fileBytes); err != nil {
			return errors.Wrapf(err, "writeFile")
		}
	}

	errorRun := conf_tool.RunCommand("./bin/csGen.exe", "--conf="+path.Dir(conf_tool.TransPath(tempConfDir)), "--csPath="+path.Dir(conf_tool.TransPath(tempCsDir)))

	if errorRun != nil {
		return errors.Errorf("EXE 执行失败:%v", errorRun)
	}

	csData, err := conf_tool.ZipFiles(tempCsDir)

	if err != nil {
		return errors.Errorf("zipXlsxFiles fail, err: %v", err)
	}

	fmt.Println("csData:", len(csData))

	configJson.CsBody = csData

	return nil
}
