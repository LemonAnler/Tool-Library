package main

import (
	"Tool-Library/components/SqliteDBGen"
	"Tool-Library/components/VersionTxtGen"
	excel_to_proto "Tool-Library/components/excel-to-proto"
	"Tool-Library/components/filemode"
	"flag"
	"fmt"
	"os"
	"time"
)

var genPath = "./gen/exceltodb/"

var ProtoPath = "./gen/proto/" //指定到同一个文件夹下面

func main() {
	confPath := "conf"
	flag.StringVar(&confPath, "conf", confPath, "Conf 路径")

	genDBPath := genPath + "db/"
	flag.StringVar(&genDBPath, "dbPath", genDBPath, "DB 路径")

	flag.Parse()

	fmt.Println("数据库生成路径：", genDBPath)
	fmt.Println("配置表路径：", confPath)

	startTime := time.Now()

	errorMkdir := filemode.MkdirAll(genPath, os.ModePerm)
	if errorMkdir != nil {
		fmt.Println("创建gen目录失败 Err:", errorMkdir)
		return
	}

	errorMkdir = filemode.MkdirAll(ProtoPath, os.ModePerm)
	if errorMkdir != nil {
		fmt.Println("创建ProtoPath目录失败 Err:", errorMkdir)
		return
	}

	timeGenProto := time.Now()
	//转表生成proto
	if errExcelToProto := excel_to_proto.GenerateExcelToProto(confPath, ProtoPath+"proto_id.yaml", ProtoPath); errExcelToProto != nil {
		fmt.Println("转表生成proto失败 ExcelToProtoGen.GenerateExcelToProto Err: ", errExcelToProto)
		return
	}
	costTimeGenProto := time.Since(timeGenProto)

	allDbVersion := []VersionTxtGen.MsgToDB{}

	timeDB := time.Now()
	//生成数据库
	errDB := SqliteDBGen.GenerateSqliteDB(confPath, ProtoPath, genDBPath, &allDbVersion)
	costTimeDB := time.Since(timeDB)

	if errDB != nil {
		fmt.Println("生成数据库失败：Err", errDB)
		return
	}

	//生成版本号文件
	timeVersion := time.Now()
	errVersion := VersionTxtGen.GenerateVersionFile(genDBPath, allDbVersion)
	costTimeVersion := time.Since(timeVersion)
	if errVersion != nil {
		fmt.Println("生成版本号文件失败：Err", errVersion)
		return
	}

	fmt.Println("生成proto耗时：", costTimeGenProto)
	fmt.Println("生成数据库耗时：", costTimeDB)
	fmt.Println("生成版本号文件耗时：", costTimeVersion)

	fmt.Println("总体时间：", time.Since(startTime))
}
