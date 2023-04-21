package VersionTxtGen

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"os"
)

type MsgToDB struct {
	MsgName   string
	FileName  string
	TableName string
	SheetName string
}

type VersionText struct {
	CellList []MsgToDB
}

func GenerateVersionFile(dbPath string, allDbVersion []MsgToDB) error {

	fmt.Println("--------开始生成版本文件--------")

	_, errVersionStat := os.Stat(dbPath + "version.txt")

	if !os.IsNotExist(errVersionStat) {
		fmt.Println("存在旧版本文件，删除旧版本文件")
		os.Remove(dbPath + "version.txt")
	} else {
		fmt.Println("不存在旧版本文件，开始生成新版本文件")
	}

	newVersion, errNew := os.Create(dbPath + "version.txt")

	if errNew != nil {
		return errors.Errorf("创建版本文件失败 %v", errNew)
	}

	defer newVersion.Close()

	fmt.Println("写入版本文件：条目数量：", len(allDbVersion))

	fileBytes, errJson := json.Marshal(VersionText{
		CellList: allDbVersion,
	})

	if errJson != nil {
		return errors.Errorf("生成版本文件失败  json.Marshal: %v", errJson)
	}

	newVersion.WriteString(string(fileBytes))

	fmt.Println("--------版本文件生成结束--------")

	return nil
}
