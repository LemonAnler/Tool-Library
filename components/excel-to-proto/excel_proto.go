package excel_to_proto

import (
	"Tool-Library/components/ProtoIDGen"
	conf_tool "Tool-Library/components/conf-tool"
	"Tool-Library/components/filemode"
	"Tool-Library/components/md5"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/tealeg/xlsx"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var starReadLine = 5

var ProtoVersionName = "ProtoVersion.json"

type ProtoVersion struct {
	ExcelMd5  string
	ProtoName []string
}

func GenerateExcelToProto(confPath string, idGenPath string, ProtoPath string) error {

	fmt.Println("--------开始加载ProtoID记录--------")
	//加载旧ProtoID表进来
	timeLoad := time.Now()
	protoIdGen, errLoadGen := ProtoIDGen.LoadGen(idGenPath)
	fmt.Println("加载ProtoID记录耗时：", time.Since(timeLoad))

	if errLoadGen != nil {
		return errors.Errorf("加载ProtoID记录失败，idGenNamePath：%v  Err:%v ", idGenPath, errLoadGen)
	}

	fmt.Println("--------加载ProtoID记录成功--------")

	fmt.Println("--------开始加载ProtoMd5Json--------")

	protoVersionPath := ProtoPath + ProtoVersionName

	protoVersionJson, errProtoVersion := os.ReadFile(protoVersionPath)
	if errProtoVersion != nil && os.IsNotExist(errProtoVersion) {
		//不存在直接创建
		fmt.Println("对应路径下不存在ProtoVersion，直接创建,路径：", protoVersionPath)
		fp, errCreate := os.Create(idGenPath) // 如果文件已存在，会将文件清空。
		if errCreate != nil {
			return errors.Errorf("创建在对应路径下不存在ProtoVersion失败，Err: %v", errCreate)
		}

		protoVersionJson, errProtoVersion = os.ReadFile(idGenPath)
		if errProtoVersion != nil {
			return errors.Errorf("创建在ProtoID记录后，重新读取失败: %v", errProtoVersion)
		}
		// defer延迟调用
		defer fp.Close() //关闭文件，释放资源。
	}

	protoVersionData := map[string]ProtoVersion{}

	json.Unmarshal(protoVersionJson, &protoVersionData)

	fmt.Println("--------加载ProtoMd5Json结束--------")

	fmt.Println("--------开始生成Proto--------")

	//加载表去生成对应proto
	timeGenerate := time.Now()
	errGenerate := ReadDirToGenerateProto(protoIdGen, confPath, ProtoPath, "", protoVersionData)
	fmt.Println("生成Proto耗时：", time.Since(timeGenerate))

	if errGenerate != nil {
		return errors.Errorf("生成proto失败 Err:%v ", errGenerate)
	}

	fmt.Println("--------proto生成结束--------")

	ProtoIDGen.SaveGen(protoIdGen, idGenPath)

	fmt.Println("更新记录ProtoVersion数量：")
	jsonBytes, errJson := json.Marshal(protoVersionData)

	if errJson != nil {
		return errors.Errorf("序列化protoVersionData报错 json.Marshal: %v", errJson)
	}

	fileProtoVersion, errNewProtoVersion := os.OpenFile(protoVersionPath, os.O_CREATE|os.O_TRUNC, 0777)
	defer fileProtoVersion.Close()

	if errNewProtoVersion != nil {
		return errors.Errorf("写入版本文件失败path:%v  os.OpenFile: %v", protoVersionPath, errNewProtoVersion)
	}

	fileProtoVersion.Write(jsonBytes)

	return nil
}

func ReadDirToGenerateProto(protoIdGen *ProtoIDGen.ProtoIdGen, confPath string, ProtoPath string, csPath string, protoVersionData map[string]ProtoVersion) error {
	errorCreateProto := filemode.MkdirAll(ProtoPath, 777)

	if errorCreateProto != nil {
		return errors.Errorf("创建proto目录失败 Err:%v", errorCreateProto)
	}

	//清除原来的Proto文件夹直接重新生成

	if csPath != "" {
		_, errCsIsExist := os.Stat(csPath)

		if !os.IsNotExist(errCsIsExist) {
			fmt.Println("删除原来的cs文件夹")
			os.RemoveAll(csPath)
		}

		errorCreateCs := filemode.MkdirAll(csPath, 777)

		if errorCreateCs != nil {
			return errors.Errorf("创建cs目录失败 Err:%v", errorCreateCs)
		}
	}

	if strings.HasSuffix(confPath, "/") {
		confPath = confPath[:len(confPath)-1]
	}
	dirWithSep := confPath + "/"

	if fss, err := os.ReadDir(dirWithSep); err != nil {
		return errors.Errorf("GenerateProto error 生成失败读取文件, %v", err)
	} else {

		wg := &sync.WaitGroup{}
		var loadErrorRef atomic.Value

		loadMux := &sync.Mutex{}
		timeGen := time.Now()
		for _, f := range fss {

			fName := f.Name()

			if filepath.Ext(fName) != ".xlsx" {
				continue
			}

			if strings.Contains(fName, "~$") {
				continue
			}

			path := dirWithSep + fName
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if errRecover := recover(); errRecover != nil {
						fmt.Println("GenerateProto error 生成失败,Err:", errRecover)
						debug.PrintStack()
					}
				}()
				loadMux.Lock()
				defer loadMux.Unlock()

				errGen := genProtoByTable(path, ProtoPath, csPath, protoIdGen, protoVersionData)

				if errGen != nil {
					loadErrorRef.Store(errors.Errorf("genProtoByTable 表格：%v 生成Proto失败 Error：%v", path, errGen))
					return
				}
			}()
		}

		wg.Wait()
		fmt.Println("多线程生成Proto耗时：", time.Since(timeGen))

		if loadError := loadErrorRef.Load(); loadError != nil {
			return errors.Errorf("多线程生成Proto,error, %v", loadError)
		}
	}

	return nil
}

func genProtoByTable(path string, ProtoPath string, csPath string, protoIdGen *ProtoIDGen.ProtoIdGen, protoVersionData map[string]ProtoVersion) error {

	data, errFileTable := os.ReadFile(path)

	if errFileTable != nil {
		return errors.Errorf("GenerateProto error 读取文件失败,path:%v %v", path, errFileTable)
	}

	//判定是否继续生成

	//获取文件名带后缀
	filenameWithSuffix := filepath.Base(path)
	//获取文件后缀
	fileSuffix := filepath.Ext(path)
	//获取文件名
	filenameOnly := strings.TrimSuffix(filenameWithSuffix, fileSuffix)

	needGen := true

	if _, isOk := protoVersionData[filenameOnly]; isOk {
		v := protoVersionData[filenameOnly]

		if v.ExcelMd5 == md5.String(data) {
			needGen = false
			//虽然不需要读取数据了，但是 cs还是需要生成

			if csPath != "" {

				for _, protoName := range v.ProtoName {
					errRun := conf_tool.RunCommand("protoc", "--csharp_out="+csPath, ProtoPath+protoName)

					if errRun != nil {
						return errors.Errorf("本次版本生成cs文件失败 %v", errRun)
					}
				}
			}
		}
	}

	if !needGen {
		return nil
	}

	fmt.Println("开始生成表格Proto：", path)

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

		messageName := ProtoIDGen.GetMessageName(filenameOnly, sheetName)

		builder := strings.Builder{}

		itselfProto := ProtoPath + messageName + ".proto"

		if _, err := os.Stat(itselfProto); err == nil {
			errRemove := os.Remove(itselfProto)
			if errRemove != nil {
				return errors.Errorf("删除proto文件失败 %v", errRemove)
			}
		}

		fileProto, errNewProto := os.OpenFile(itselfProto, os.O_CREATE, 0777)
		fileProto.Close()

		if errNewProto != nil {
			return errors.Errorf("创建proto文件失败 %v", errNewProto)
		}

		//消息头部
		_, err := builder.WriteString("syntax = \"proto3\";\n\npackage conf;\n\noption go_package=\"" + ProtoPath + ";conf\";")

		if err != nil {
			return errors.Errorf("GenItselfProtoStr builder.WriteString Proto Head Err:%v", err)
		}

		//写入proto文件
		errGenProto := GenProtoTomessage(path, sheetName, memberMap, &builder, protoIdGen)

		if errGenProto != nil {
			return errors.Errorf("GenProtoTomessage 生成各自 proto失败 %v", errGenProto)
		}

		errWrite := os.WriteFile(ProtoPath+messageName+".proto", []byte(builder.String()), 0777)

		if errWrite != nil {
			return errors.Errorf("写入各自proto文件失败 %v", errWrite)
		}

		if csPath != "" {
			errRun := conf_tool.RunCommand("protoc", "--csharp_out="+csPath, ProtoPath+messageName+".proto")

			if errRun != nil {
				return errors.Errorf("本次版本生成cs文件失败 %v", errRun)
			}
		}

		protoNameList := protoVersionData[filenameOnly].ProtoName

		protoNameList = append(protoNameList, messageName+".proto")

		protoVersionData[filenameOnly] = ProtoVersion{
			ExcelMd5:  md5.String(data),
			ProtoName: protoNameList,
		}
	}

	return nil
}

func GenItselfProtoStr(protoPath string, messageName string, builder *strings.Builder) error {

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
