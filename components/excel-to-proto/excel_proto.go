package excel_to_proto

import (
	"Tool-Library/components/ProtoIDGen"
	"fmt"
	"github.com/pkg/errors"
	"github.com/tealeg/xlsx"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

var starReadLine = 5

func GenerateExcelToProto(isUpdateConf bool, confPath string, idGenPath string, ProtoPath string) error {
	//更新配置

	fmt.Println("     git 自动更新关闭    ")
	if isUpdateConf {
		cmd := exec.Command("cd", "conf", "&&", "git", "reset", "--hard origin/master")

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		e := cmd.Run()
		if e != nil {
			return errors.Errorf("更新配置失败 Err:%v ", e)
		}
	}

	fmt.Println("--------开始加载ProtoID记录--------")
	//加载旧ProtoID表进来
	protoIdGen, errLoadGen := ProtoIDGen.LoadGen(idGenPath)

	if errLoadGen != nil {
		return errors.Errorf("加载ProtoID记录失败，idGenNamePath：%v  Err:%v ", idGenPath, errLoadGen)
	}

	fmt.Println("--------加载ProtoID记录成功--------")

	fmt.Println("--------开始生成Proto--------")

	//加载表去生成对应proto
	errGenerate := readDirToGenerateProto(protoIdGen, confPath, ProtoPath)

	if errGenerate != nil {
		return errors.Errorf("生成proto失败 Err:%v ", errGenerate)
	}

	fmt.Println("--------proto生成结束--------")

	ProtoIDGen.SaveGen(protoIdGen, idGenPath)

	return nil
}

func readDirToGenerateProto(protoIdGen *ProtoIDGen.ProtoIdGen, confPath string, ProtoPath string) error {

	confProtoPath := ProtoPath + "confpa.proto"
	//清除原来的Proto直接重新生成
	_, errProtoIsExist := os.Stat(confProtoPath)

	if !os.IsNotExist(errProtoIsExist) {
		os.Remove(confProtoPath)
	}

	fileProto, errNewProto := os.OpenFile(confProtoPath, os.O_CREATE, 0777)
	defer fileProto.Close()

	if errNewProto != nil {
		return errors.Errorf("创建proto文件失败 %v", errNewProto)
	}

	protoStr := strings.Builder{}

	//消息头部

	_, errProtoStr := protoStr.WriteString("syntax = \"proto3\";\n\npackage conf;\n\noption go_package=\"" + confProtoPath + ";conf\";")

	if errProtoStr != nil {
		return errors.Errorf("protoStr.WriteString 写入proto文件失败 %v", errProtoStr)
	}

	if strings.HasSuffix(confPath, "/") {
		confPath = confPath[:len(confPath)-1]
	}
	dirWithSep := confPath + "/"

	if fss, err := os.ReadDir(dirWithSep); err != nil {
		return errors.Errorf("GenerateProto error 生成失败读取文件, %v", err)
	} else {
		fMap := make(map[string]struct{})
		for _, f := range fss {
			fName := f.Name()

			path := dirWithSep + fName
			if filepath.Ext(fName) != ".xlsx" {
				continue
			}

			if strings.Contains(fName, "~$") {
				continue
			}

			fMap[path] = struct{}{}
		}

		fmt.Println("表格数量：", len(fMap))

		for path := range fMap {

			errGen := genProtoByTable(path, ProtoPath, &protoStr, protoIdGen)

			if errGen != nil {
				return errors.Errorf("genProtoByTable 表格：%v 生成Proto失败 Error：%v", path, errGen)
			}
		}
	}

	//写入总的proto文件
	err := os.WriteFile(confProtoPath, []byte(protoStr.String()), 0777)

	if err != nil {
		return errors.Errorf("写入protoA文件失败 %v", err)
	}

	return nil
}

func genProtoByTable(path string, ProtoPath string, allProtoBuilder *strings.Builder, protoIdGen *ProtoIDGen.ProtoIdGen) error {

	data, errFileTable := os.ReadFile(path)

	if errFileTable != nil {
		return errors.Errorf("GenerateProto error 读取文件失败,path:%v %v", path, errFileTable)
	}

	file, errOpenBinary := xlsx.OpenBinary(data)
	if errOpenBinary != nil {
		return errors.Wrapf(errOpenBinary, "解析表格数据失败 OpenBinary 表名：%s", path)
	}

	// 找到需要处理sheet（读取sheet(list)）
	ListSheet := file.Sheet["list"]
	if ListSheet == nil {
		return errors.Errorf("表格数据中没有找到list页签 表名：%s ", path)
	}

	for i := 0; i < len(ListSheet.Rows); i++ {
		sheetRow := ListSheet.Rows[i]

		if len(sheetRow.Cells) < 1 {
			continue
		}
		sheetName := sheetRow.Cells[0].String()

		curSheet := file.Sheet[strings.TrimSpace(sheetName)]

		if curSheet == nil {
			fmt.Println("找不到sheet 表名：[", path, "] sheetName  [", sheetName, "]跳过")
			continue
		}

		memberMap := make(map[string]string)

		if len(curSheet.Rows) < starReadLine {
			return errors.Errorf("表格行数不足跳过 表名：%v 页签名称：%v 行数：%d ，最低行数要求：%d \n", path, sheetName, len(curSheet.Rows), starReadLine)
		}

		titleRow := curSheet.Rows[1]
		for j := 0; j < len(titleRow.Cells); j++ {

			title := titleRow.Cells[j].String()

			if title == "" {
				continue
			}

			if len(curSheet.Rows) < 3 {
				//fmt.Printf("表格行数不足跳过 表名：%v 页签名称：%v 行数：%d \n", path, sheetName, len(curSheet.Rows))
				continue
			}

			titleType := strings.ToLower(curSheet.Rows[2].Cells[j].String())

			if titleType == "" {
				fmt.Printf("表格列类型为空跳过 表名：%v 页签名称：%v 列数：%d \n", path, sheetName, j)
				continue
			}

			protoType := getProtoType(titleType) //不会出现NULL

			if protoType != "" {
				if memberMap[title] != "" {
					if len(strings.Split(memberMap[title], " ")) == 1 {
						memberMap[title] = "repeated " + protoType
					} else {
						//已经repeated过了
					}
				} else {
					memberMap[title] = protoType
				}
			}
		}

		//获取文件名带后缀
		filenameWithSuffix := filepath.Base(path)
		//获取文件后缀
		fileSuffix := filepath.Ext(path)
		//获取文件名
		filenameOnly := strings.TrimSuffix(filenameWithSuffix, fileSuffix)

		messageName := ProtoIDGen.GetMessageName(filenameOnly, sheetName)

		itselfProtoStr := strings.Builder{}

		errItself := GenItselfProtoStr(ProtoPath, messageName, &itselfProtoStr)

		if errItself != nil {
			return errors.Errorf("GenItselfProtoStr 生成自身proto Str失败 %v", errItself)
		}

		//写入proto文件
		errGenProto := GenProtoTomessage(path, sheetName, memberMap, &itselfProtoStr, protoIdGen)

		if errGenProto != nil {
			return errors.Errorf("GenProtoTomessage 生成各自 proto失败 %v", errGenProto)
		}

		fmt.Println("写入各自proto文件：", ProtoPath+messageName+".proto")

		errWrite := os.WriteFile(ProtoPath+messageName+".proto", []byte(itselfProtoStr.String()), 0777)

		if errWrite != nil {
			return errors.Errorf("写入各自proto文件失败 %v", errWrite)
		}

		errGenProto = GenProtoTomessage(path, sheetName, memberMap, allProtoBuilder, protoIdGen)

		if errGenProto != nil {
			return errors.Errorf("GenProtoTomessage 生成总体 proto失败 %v", errGenProto)
		}
	}

	return nil
}

func GenItselfProtoStr(protoPath string, messageName string, builder *strings.Builder) error {

	if strings.HasSuffix(protoPath, "confpb.proto") == true {
		protoPath = protoPath[:len(protoPath)-12]
	}

	itselfProto := protoPath + messageName + ".proto"

	if _, err := os.Stat(itselfProto); err == nil {
		errRemove := os.Remove(itselfProto)
		if errRemove != nil {
			return errors.Errorf("删除proto文件失败 %v", errRemove)
		}
	}

	fileProto, errNewProto := os.OpenFile(itselfProto, os.O_CREATE, 0777)

	if errNewProto != nil {
		return errors.Errorf("创建proto文件失败 %v", errNewProto)
	}

	fileProto.Close()

	//消息头部
	_, err := builder.WriteString("syntax = \"proto3\";\n\npackage conf;\n\noption go_package=\"" + protoPath + ";conf\";")

	if err != nil {
		return errors.Errorf("GenItselfProtoStr builder.WriteString Proto Head Err:%v", err)
	}

	return nil
}

func GenProtoTomessage(path string, sheetName string, memberMap map[string]string, builder *strings.Builder, protoIdGen *ProtoIDGen.ProtoIdGen) error {
	//获取文件名带后缀
	filenameWithSuffix := filepath.Base(path)
	//获取文件后缀
	fileSuffix := filepath.Ext(path)
	//获取文件名
	filenameOnly := strings.TrimSuffix(filenameWithSuffix, fileSuffix)

	messageName := ProtoIDGen.GetMessageName(filenameOnly, sheetName)

	_, errProtoStr := builder.WriteString("\nmessage  " + messageName + "   {\n\n")

	if errProtoStr != nil {
		return errors.Errorf("builder.WriteString Proto Head Err:%v", errProtoStr)
	}

	for k, v := range memberMap {
		strColType := v
		if strings.HasPrefix(strColType, "repeated") {
			strColType = strings.Trim(strings.TrimSpace(strColType), "repeated") + "_array"
		}

		fieldName := sheetName + ProtoIDGen.KeySep + k + ProtoIDGen.KeySep + strColType

		writeStr := "    " + v + "   " + k + " = " + strconv.Itoa(protoIdGen.GetTypeFieldId(filenameOnly, fieldName)) + ";\n\n"

		_, errProtoStr = builder.WriteString(writeStr)

		if errProtoStr != nil {
			return errors.Errorf("builder.WriteString Proto Body Err:%v", errProtoStr)
		}
	}

	_, errProtoStr = builder.WriteString("}\n")

	if errProtoStr != nil {
		return errors.Errorf("builder.WriteString Proto End Err:%v", errProtoStr)
	}

	return nil
}

func getProtoType(titleType string) string {
	protoType := "string"

	if titleType == "bool" {
		protoType = "bool"
	}

	if titleType == "float" {
		protoType = "float"
	}

	if titleType == "int" {
		protoType = "int32"
	}

	typeArray := strings.Split(titleType, "_")

	if len(typeArray) == 2 && typeArray[1] == "list" {
		protoType = "repeated " + protoType
	}

	return protoType
}
