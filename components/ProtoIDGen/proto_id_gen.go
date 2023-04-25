package ProtoIDGen

import (
	"fmt"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"os"
	"strings"
)

// ProtoID
func LoadGen(idGenPath string) (*ProtoIdGen, error) {
	fmt.Println("开始加ProtoID记录，对应路径：", idGenPath)
	idData, errRead := os.ReadFile(idGenPath)
	if errRead != nil && os.IsNotExist(errRead) {
		//不存在直接创建
		fmt.Println("对应路径下不存在ProtoID记录，直接创建", idGenPath)

		fp, errCreate := os.Create(idGenPath) // 如果文件已存在，会将文件清空。
		if errCreate != nil {
			return nil, errors.Errorf("创建在ProtoID记录失败，Err: %v", errCreate)
		}

		idData, errRead = os.ReadFile(idGenPath)
		if errRead != nil {
			return nil, errors.Errorf("创建在ProtoID记录后，重新读取失败: %v", errRead)
		}
		// defer延迟调用
		defer fp.Close() //关闭文件，释放资源。
	}

	return newGen(idData)
}

func newGen(data []byte) (*ProtoIdGen, error) {
	g := &ProtoIdGen{
		idMap: map[string]int{},
	}

	if len(data) > 0 {
		err := yaml.Unmarshal(data, g.idMap)
		if err != nil {
			return nil, errors.Wrap(err, "yaml.unmarshal fail")
		}
	}

	return g, nil
}

func SaveGen(protoIdGen *ProtoIdGen, idGenNamePath string) error {
	if protoIdGen == nil {
		return nil
	}

	data, errorEncode := protoIdGen.encode()

	if errorEncode != nil {
		return errorEncode
	}

	err := os.WriteFile(idGenNamePath, data, 0777)

	if err != nil {
		return errors.Errorf("保存ProtoID记录失败，idGenNamePath：%v  Err:%v ", idGenNamePath, err)
	}

	return nil
}

type ProtoIdGen struct {
	idMap map[string]int
}

func (g *ProtoIdGen) encode() ([]byte, error) {
	return yaml.Marshal(g.idMap)
}

func (g *ProtoIdGen) get(key string) (int, bool) {
	id, ok := g.idMap[key]
	return id, ok
}

func (g *ProtoIdGen) getOrCreate(key, newKey string) int {
	id, ok := g.idMap[key]
	if !ok {
		id = g.newId(newKey)
		g.idMap[key] = id
		return id
	}

	return id
}

func (g *ProtoIdGen) newId(key string) int {
	id, ok := g.idMap[key]
	if !ok {
		g.idMap[key] = 1
		return 1
	}

	g.idMap[key] = id + 1
	return id + 1
}

func (g *ProtoIdGen) GetConfigFieldId(name string) int {
	return g.getOrCreate(ConfigFieldIdKey(name))
}

func (g *ProtoIdGen) GetTypeFieldId(typeName, fieldName string) int {
	return g.getOrCreate(TypeFieldIdKey(typeName, fieldName))
}

const KeySep = "#"
const ConfigFieldIdPrefix = "ConfigField" + KeySep

func ConfigFieldIdKey(name string) (string, string) {
	return ConfigFieldIdPrefix + HumpName(name), ConfigFieldIdPrefix
}

const TypeFieldIdPrefix = "TypeField" + KeySep

func TypeFieldIdKey(typeName, fieldName string) (string, string) {
	prefix := TypeFieldIdPrefix + HumpName(typeName) + KeySep
	return prefix + HumpName(fieldName), prefix
}

func HumpName(in string) string {
	return strings.Replace(strings.Title(strings.Replace(in, "_", " ", -1)), " ", "", -1)
}

// 公共调用方法
func GetMessageName(filenameOnly string, sheetName string) string {
	return "confpb" + filenameOnly + sheetName
}
