package main

import (
	"Tool-Library/components/ProtoIDGen"
	excel_to_proto "Tool-Library/components/excel-to-proto"
	"Tool-Library/components/filemode"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {

	genPath := "./gen/cs-gen/"

	confPath := "conf"
	flag.StringVar(&confPath, "conf", confPath, "指定配置表格路径")

	csPath := genPath + "cs/"
	flag.StringVar(&csPath, "csPath", csPath, "指定CS生成路径")

	flag.Parse()

	idGenPath := genPath + "proto_id.yaml"
	ProtoPath := genPath + "proto/"

	timeCost := time.Now()

	errCreateGen := filemode.MkdirAll(genPath, 777)

	if errCreateGen != nil {
		fmt.Println("创建gen目录失败 Err:", errCreateGen)
		return
	}

	//加载旧ProtoID表进来
	protoIdGen, errLoadGen := ProtoIDGen.LoadGen(idGenPath)

	if errLoadGen != nil {
		fmt.Printf("加载ProtoID记录失败，idGenNamePath：%v  Err:%v ", idGenPath, errLoadGen)
		return
	}

	protoVersionPath := ProtoPath + excel_to_proto.ProtoVersionName

	protoVersionJson, errProtoVersion := os.ReadFile(protoVersionPath)
	if errProtoVersion != nil && os.IsNotExist(errProtoVersion) {
		//不存在直接创建
		fmt.Println("对应路径下不存在ProtoVersion，直接创建,路径：", protoVersionPath)
		fp, errCreate := os.Create(idGenPath) // 如果文件已存在，会将文件清空。
		if errCreate != nil {
			fmt.Printf("创建在对应路径下不存在ProtoVersion失败，Err: %v", errCreate)
			return
		}

		protoVersionJson, errProtoVersion = os.ReadFile(idGenPath)
		if errProtoVersion != nil {
			fmt.Printf("创建在ProtoID记录后，重新读取失败: %v", errProtoVersion)
			return
		}
		// defer延迟调用
		defer fp.Close() //关闭文件，释放资源。
	}

	protoVersionData := map[string]excel_to_proto.ProtoVersion{}

	json.Unmarshal(protoVersionJson, &protoVersionData)

	fmt.Println("--------加载ProtoMd5Json结束--------")

	fmt.Println("--------开始生成Proto--------")

	//加载表去生成对应proto
	timeGenerate := time.Now()
	errGenerate := excel_to_proto.ReadDirToGenerateProto(protoIdGen, confPath, ProtoPath, csPath, protoVersionData)
	fmt.Println("生成Proto耗时：", time.Since(timeGenerate))

	if errGenerate != nil {
		fmt.Printf("生成proto失败 Err:%v ", errGenerate)
		return
	}

	fmt.Println("--------proto生成结束--------")

	ProtoIDGen.SaveGen(protoIdGen, idGenPath)

	jsonBytes, errJson := json.Marshal(protoVersionData)

	if errJson != nil {
		fmt.Printf("序列化protoVersionData报错 json.Marshal: %v", errJson)
		return
	}

	fileProtoVersion, errNewProtoVersion := os.OpenFile(protoVersionPath, os.O_CREATE|os.O_TRUNC, 0777)
	defer fileProtoVersion.Close()

	if errNewProtoVersion != nil {
		fmt.Printf("写入版本文件失败path:%v  os.OpenFile: %v", protoVersionPath, errNewProtoVersion)
		return
	}

	fileProtoVersion.Write(jsonBytes)

	fmt.Println("程序耗时：", time.Since(timeCost))
}
