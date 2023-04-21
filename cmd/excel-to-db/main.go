package main

import (
	"Tool-Library/components/SqliteDBGen"
	"Tool-Library/components/VersionTxtGen"
	conf_tool "Tool-Library/components/conf-tool"
	"Tool-Library/components/excel-to-proto"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"os"
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

	startTime := time.Now()

	errorMkdir := os.MkdirAll(genPath, os.ModePerm)
	if errorMkdir != nil {
		fmt.Println("创建gen目录失败 Err:", errorMkdir)
		return
	}

	errorCreateProto := os.MkdirAll(ProtoPath, 777)

	if errorCreateProto != nil {
		fmt.Println("创建proto目录失败 Err:", errorCreateProto)
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

	errorMkdir := os.MkdirAll(csPath, 777)
	if errorMkdir != nil {
		return errors.Errorf("创建cs目录失败 Err:%v ", errorMkdir)
	}

	conf_tool.RunCommand("pwd")

	conf_tool.RunCommand("csPath", csPath, ProtoPath+"confpb*.proto")

	err := conf_tool.RunCommand("protoc", "--csharp_out="+csPath, ProtoPath+"confpb*.proto")

	if err != nil {
		return errors.Errorf("生成cs失败 Err:%v ", err)
	}

	fmt.Println("\n--------cs生成结束--------")

	return nil
}
