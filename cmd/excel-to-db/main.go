package main

import (
	"Tool-Library/components/SqliteDBGen"
	"Tool-Library/components/VersionTxtGen"
	conf_tool "Tool-Library/components/conf-tool"
	excel_to_proto "Tool-Library/components/excel-to-proto"
	"Tool-Library/components/filemode"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var genPath = "./gen/exceltodb/"

var idGenPath = genPath + "proto_id.yaml"

var ProtoPath = genPath + "proto/"

var updateConf = false

var isCleanDB = false

var isOpenGenCs = true

func main() {

	//指定初始化参数
	genDBPath := genPath + "db/"
	flag.StringVar(&genDBPath, "dbPath", genDBPath, "数据库生成指定路径路径")

	csPath := genPath + "cs/"
	flag.StringVar(&csPath, "csPath", "./gen/cs/", "cs生成指定路径路径")

	confPath := "conf"
	flag.StringVar(&confPath, "conf", confPath, "cs生成指定路径路径")

	isTest := false
	flag.BoolVar(&isTest, "isTest", isTest, "是否进行测试")
	flag.Parse()

	fmt.Println("数据库生成路径：", genDBPath)
	fmt.Println("cs生成路径：", csPath)
	fmt.Println("配置表路径：", confPath)
	fmt.Println("Proto路径：", ProtoPath)

	startTime := time.Now()

	errorMkdir := filemode.MkdirAll(genPath, os.ModePerm)
	if errorMkdir != nil {
		fmt.Println("创建gen目录失败 Err:", errorMkdir)
		return
	}

	//转表生成proto
	if errExcelToProto := excel_to_proto.GenerateExcelToProto(updateConf, confPath, idGenPath, ProtoPath); errExcelToProto != nil {
		fmt.Println("转表生成proto失败 ExcelToProtoGen.GenerateExcelToProto Err: ", errExcelToProto)
		return
	}

	//生成前端cs
	if isOpenGenCs {
		if errProtoToCs := GenerateProtoToCs(csPath, ProtoPath); errProtoToCs != nil {
			fmt.Println("生成前端cs失败 Err: ", errProtoToCs)
			return
		}
	}

	//清理数据库
	if isCleanDB {
		errCleanDB := os.RemoveAll(genDBPath)
		if errCleanDB != nil {
			fmt.Println("清理数据库失败：Eer", errCleanDB)
			return
		}
	}

	allDbVersion := []VersionTxtGen.MsgToDB{}

	//生成数据库
	errDB := SqliteDBGen.GenerateSqliteDB(confPath, ProtoPath+"confpa.proto", genDBPath, &allDbVersion)

	if errDB != nil {
		fmt.Println("生成数据库失败：Err", errDB)
		return
	}

	//生成版本号文件
	errVersion := VersionTxtGen.GenerateVersionFile(genDBPath, allDbVersion)

	if errVersion != nil {
		fmt.Println("生成版本号文件失败：Err", errVersion)
		return
	}

	fmt.Println("总体时间：", time.Since(startTime))

	//SqliteDBGen.SqliteTest(confPath, genDBPath, ProtoPath+"confpa.proto", renameDBList[5])
}

func GenerateProtoToCs(csPath string, ProtoPath string) error {
	fmt.Println("\n --------cs生成开始--------")

	errorMkdir := filemode.MkdirAll(csPath, 777)
	if errorMkdir != nil {
		return errors.Errorf("创建cs目录失败 Err:%v ", errorMkdir)
	}

	fs, err := os.ReadDir(ProtoPath)
	if err != nil {
		return errors.Wrapf(err, "读取文件夹失败，root: %s", ProtoPath)
	}

	wg := &sync.WaitGroup{}
	var loadErrorRef atomic.Value

	loadMux := &sync.Mutex{}

	for _, f := range fs {

		if f.Name() == "confpa.proto" || f.IsDir() {
			continue
		}

		path := ProtoPath + f.Name()

		fmt.Println("生成CS文件:", f.Name())

		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() {
				if errRecover := recover(); errRecover != nil {
					fmt.Println("GenerateProto error 生成失败,Err:", errRecover)
					debug.PrintStack()
				}
			}()

			errRun := conf_tool.RunCommand("protoc", "--csharp_out="+csPath, path)

			fmt.Println(path)

			if errRun != nil {
				loadErrorRef.Store(errors.Errorf("生成CS失败:%v", errRun))
				return
			}

			loadMux.Lock()
			defer loadMux.Unlock()
		}()
	}

	wg.Wait()

	if loadError := loadErrorRef.Load(); loadError != nil {
		return errors.Errorf("多线程生成cs,error, %v", loadError)
	}

	fmt.Println("\n--------cs生成结束--------")

	return nil
}
