package config

import (
	"bytes"
	"embed"
	"fmt"
	"github.com/axgle/mahonia"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tealeg/xlsx"
	unicode16 "golang.org/x/text/encoding/unicode"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

type GameObjects struct {
	dataMap map[string][]byte

	gbkDecoder    mahonia.Decoder
	decoderLocker sync.Mutex
}

func (g *GameObjects) Walk(walkFunc func(path string, data []byte)) {
	for path, data := range g.dataMap {
		walkFunc(path, data)
	}
}

func (g *GameObjects) Data(filename string) string {
	return g.bytes2string(g.Bytes(filename))
}

func (g *GameObjects) Bytes(filename string) []byte {
	return g.dataMap[filename]
}

func (g *GameObjects) Exist(filename string) bool {
	_, exist := g.dataMap[filename]
	return exist
}

func (g *GameObjects) bytes2string(data []byte) string {
	if len(data) <= 0 || utf8.Valid(data) {
		return string(data)
	}

	g.decoderLocker.Lock()
	defer g.decoderLocker.Unlock()
	return g.gbkDecoder.ConvertString(string(data))
}

func (g *GameObjects) LoadFile(filename string) ([]*ObjectParser, error) {
	var result []*ObjectParser

	for _, name := range strings.Split(filename, ";") {
		parseResult, err := ParseList(name, g.Data(name))
		if err != nil {
			return nil, err
		}

		result = append(result, parseResult...)
	}

	return result, nil
}

func (g *GameObjects) LoadFileWithHeads(filename string) ([]*ObjectParser, []string, error) {
	var result []*ObjectParser

	headMap := make(map[string]*headWithIndex)
	headCounter := 0
	for _, name := range strings.Split(filename, ";") {
		parseResult, heads, err := ParseListWithHeads(name, g.Data(name))
		if err != nil {
			return nil, nil, err
		}

		result = append(result, parseResult...)

		for _, v := range heads {
			headName := strings.ToLower(v)
			if headMap[headName] == nil {
				head := &headWithIndex{}
				head.name = headName
				head.idx = headCounter
				headCounter++
				headMap[headName] = head
			}
		}
	}

	his := make([]*headWithIndex, 0, len(headMap))
	for _, v := range headMap {
		his = append(his, v)
	}
	sort.Slice(his, func(i, j int) bool {
		return his[i].idx < his[j].idx
	})

	headResult := make([]string, 0, len(his))
	for _, v := range his {
		headResult = append(headResult, v.name)
	}

	return result, headResult, nil
}

type headWithIndex struct {
	name string
	idx  int
}

func (g *GameObjects) LoadKvFile(filename string) (*ObjectParser, error) {
	return ParseKv(filename, g.Data(filename))
}

var allEmbed *embed.FS

func SetAllEmbed(fs *embed.FS) {
	allEmbed = fs
}

var confEmbed *embed.FS

func SetConfEmbed(fs *embed.FS) {
	confEmbed = fs
}

func NewEmbedGameObjects(dir string) (*GameObjects, error) {

	if confEmbed == nil {
		return NewConfigGameObjects(dir)
	}

	gos := &GameObjects{}
	gos.dataMap = make(map[string][]byte)
	gos.gbkDecoder = mahonia.NewDecoder("gbk")

	decoder := mahonia.NewDecoder("gbk")

	utf16leDecoder := unicode16.UTF16(unicode16.LittleEndian, unicode16.UseBOM).NewDecoder()
	utf16beDecoder := unicode16.UTF16(unicode16.BigEndian, unicode16.UseBOM).NewDecoder()

	logrus.Debugf("解析文件配置!")

	err1 := fs.WalkDir(confEmbed, dir, func(path0 string, info fs.DirEntry, err error) error {

		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		dp := strings.Replace(path0, "\\", "/", -1) // 更新windows支持
		dp = strings.TrimPrefix(dp, strings.Replace(dir, "\\", "/", -1))
		if strings.HasPrefix(dp, "/") {
			dp = dp[1:]
		}

		if strings.Contains(dp, "~$") {
			return nil
		}

		data, err1 := confEmbed.ReadFile(path0)
		if err1 != nil {
			return errors.Wrapf(err1, "read config fail, %s", path0)
		}

		if strings.HasSuffix(dp, ".xlsx") {
			result, err := parseExcelFile(dp, data)
			if err != nil {
				return errors.Wrapf(err, "解析excel文件出错: %s", dp)
			}

			for p, v := range result {
				if !utf8.Valid(v) {
					gos.dataMap[p] = []byte(decoder.ConvertString(string(v)))
				} else {
					gos.dataMap[p] = v
				}
			}

			return nil
		}

		if strings.HasSuffix(dp, ".json") {
			result, err := parseJsonFile(dp, data)
			if err != nil {
				return errors.Wrapf(err, "解析json文件出错: %s", dp)
			}

			for p, v := range result {
				if !utf8.Valid(v) {
					gos.dataMap[p] = []byte(decoder.ConvertString(string(v)))
				} else {
					gos.dataMap[p] = v
				}
			}

			return nil
		}

		// 尝试处理utf16编码格式
		if len(data) >= 2 {

			src := data

			switch {
			case src[0] == 0xfe && src[1] == 0xff:
				data, err = utf16beDecoder.Bytes(src)
				if err != nil {
					return errors.Wrapf(err, "解析utf16be文件出错: %s", dp)
				}
				gos.dataMap[dp] = data
				return nil
			case src[0] == 0xff && src[1] == 0xfe:
				data, err = utf16leDecoder.Bytes(src)
				if err != nil {
					return errors.Wrapf(err, "解析utf16le文件出错: %s", dp)
				}
				gos.dataMap[dp] = data
				return nil
			default:
			}
		}

		// 字符编码转换
		if !utf8.Valid(data) {
			gos.dataMap[dp] = []byte(decoder.ConvertString(string(data)))
		} else {
			gos.dataMap[dp] = data
		}
		return nil
	})

	return gos, err1
}

func NewConfigGameObjects(dir string) (*GameObjects, error) {

	gos := &GameObjects{}
	gos.dataMap = make(map[string][]byte)
	gos.gbkDecoder = mahonia.NewDecoder("gbk")

	decoder := mahonia.NewDecoder("gbk")

	utf16leDecoder := unicode16.UTF16(unicode16.LittleEndian, unicode16.UseBOM).NewDecoder()
	utf16beDecoder := unicode16.UTF16(unicode16.BigEndian, unicode16.UseBOM).NewDecoder()

	logrus.Debugf("解析文件配置!")

	err1 := filepath.Walk(dir, func(path0 string, info os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		dp := strings.Replace(path0, "\\", "/", -1) // 更新windows支持
		dp = strings.TrimPrefix(dp, strings.Replace(dir, "\\", "/", -1))
		if strings.HasPrefix(dp, "/") {
			dp = dp[1:]
		}

		if strings.Contains(dp, "~$") {
			return nil
		}

		data, err1 := ioutil.ReadFile(path0)
		if err1 != nil {
			return errors.Wrapf(err1, "read config fail, %s", path0)
		}

		if strings.HasSuffix(dp, ".xlsx") {
			result, err := parseExcelFile(dp, data)
			if err != nil {
				return errors.Wrapf(err, "解析excel文件出错: %s", dp)
			}

			for p, v := range result {
				if !utf8.Valid(v) {
					gos.dataMap[p] = []byte(decoder.ConvertString(string(v)))
				} else {
					gos.dataMap[p] = v
				}
			}

			return nil
		}

		if strings.HasSuffix(dp, ".json") {
			result, err := parseJsonFile(dp, data)
			if err != nil {
				return errors.Wrapf(err, "解析json文件出错: %s", dp)
			}

			for p, v := range result {
				if !utf8.Valid(v) {
					gos.dataMap[p] = []byte(decoder.ConvertString(string(v)))
				} else {
					gos.dataMap[p] = v
				}
			}

			return nil
		}

		// 尝试处理utf16编码格式
		if len(data) >= 2 {

			src := data

			switch {
			case src[0] == 0xfe && src[1] == 0xff:
				data, err = utf16beDecoder.Bytes(src)
				if err != nil {
					return errors.Wrapf(err, "解析utf16be文件出错: %s", dp)
				}
				gos.dataMap[dp] = data
				return nil
			case src[0] == 0xff && src[1] == 0xfe:
				data, err = utf16leDecoder.Bytes(src)
				if err != nil {
					return errors.Wrapf(err, "解析utf16le文件出错: %s", dp)
				}
				gos.dataMap[dp] = data
				return nil
			default:
			}
		}

		// 字符编码转换
		if !utf8.Valid(data) {
			gos.dataMap[dp] = []byte(decoder.ConvertString(string(data)))
		} else {
			gos.dataMap[dp] = data
		}
		return nil
	})

	return gos, err1
}

func parseExcelFile(path string, data []byte) (result map[string][]byte, err error) {
	// excel
	xlFile, err := xlsx.OpenBinary(data)
	if err != nil {
		return
	}

	result = map[string][]byte{}

	for name, sheet := range xlFile.Sheet {
		bytesBuffer := bytes.NewBuffer(nil)

		for i, row := range sheet.Rows {
			r := i + 1
			for i, cell := range row.Cells {
				c := i + 1

				text := cell.Value

				if text != "" {
					switch cell.Type() {
					case xlsx.CellTypeString:
						text = cell.String()
					case xlsx.CellTypeStringFormula:
						text, err = cell.FormattedValue()
					case xlsx.CellTypeNumeric:
						var result float64
						result, err = cell.Float()
						if result == float64(int64(result)) {
							text = fmt.Sprintf("%d", int64(result))
						} else {
							text = strconv.FormatFloat(result, 'f', -1, 64)
						}
					}
				}

				if err != nil {
					err = errors.Wrapf(err, "%s 解析失败 %d行-%d列 %v", sheet.Name, r, c, cell)
					return
				}

				bytesBuffer.WriteString(text)
				bytesBuffer.WriteString("\t")
			}

			bytesBuffer.WriteString("\r\n")
		}

		d := bytesBuffer.Bytes()
		result[path+":"+name] = d

		if len(xlFile.Sheet) == 1 {
			result[path] = d
		}
	}

	return
}

func parseJsonFile(path string, data []byte) (result map[string][]byte, err error) {

	root := jsoniter.Get(data)
	if root.ValueType() != jsoniter.ArrayValue {
		return nil, errors.Errorf("%s 文件格式不对，最外层必须是数组", path)
	}

	type Field struct {
		name         string
		nameLowSnake string
		valueType    jsoniter.ValueType
		n            int // > 0 表示数组长度
	}

	var fields []*Field
	fieldMap := make(map[string]*Field)

	// 先遍历整个Json字符串，把表头定义搞出来
	for i := 0; i < root.Size(); i++ {
		val := root.Get(i)
		if val.ValueType() != jsoniter.ObjectValue {
			return nil, errors.Errorf("%s 文件格式不对，第%v行数据第二层必须是Object, t: %v", path, i+1, val.ValueType())
		}

		for _, k := range val.Keys() {
			data := val.Get(k)
			if data.ValueType() == jsoniter.ObjectValue {
				return nil, errors.Errorf("%s 文件格式不对，第%v行数据%s字段第三层不能是Object（不能用嵌套格式）", path, i+1, k)
				//continue
			}

			if data.ValueType() == jsoniter.InvalidValue {
				return nil, errors.Errorf("%s 文件格式不对，第%v行数据%s字段第三层解析不出来", path, i+1, k)
			}

			f := fieldMap[k]
			if f == nil {
				f = &Field{}
				f.name = k
				f.nameLowSnake = toLowerSnakeCase(k)
				f.valueType = data.ValueType()
				f.n = data.Size()

				fieldMap[k] = f
				fields = append(fields, f)
			} else {
				if f.valueType != data.ValueType() {
					return nil, errors.Errorf("%s 文件格式不对，第%v行数据%s字段, 前后的数据类型对不上，prev: %v cur: %v", path, i+1, k, f.valueType, data.ValueType())
				}

				if data.Size() > 0 {
					for i := 0; i < data.Size(); i++ {
						itemType := data.Get(i).ValueType()
						if itemType == jsoniter.ObjectValue {
							return nil, errors.Errorf("%s 文件格式不对，第%v行数据%s字段数组元素不能是Object（不能用嵌套格式）", path, i+1, k)
						}

						if itemType == jsoniter.InvalidValue {
							return nil, errors.Errorf("%s 文件格式不对，第%v行数据%s字段数组元素解析不出来", path, i+1, k)
						}
					}

					if f.n < data.Size() {
						f.n = data.Size()
					}
				}
			}
		}
	}

	b := bytes.Buffer{}

	isFirst := true
	writeField := func(text string) {
		// 做点转换
		if strings.Contains(text, "\t") {
			logrus.WithField("text", text).Errorf("发现配置中存在Tab，转换成2个空格")
			text = strings.ReplaceAll(text, "\t", "  ")
		}

		if isFirst {
			isFirst = false
		} else {
			b.WriteString("\t")
		}
		b.WriteString(text)
	}

	writeNewLine := func() {
		b.WriteString("\r\n")
		isFirst = true
	}

	for i := 0; i < 2; i++ {
		// 头2行是head，第一行描述，第二行字段名
		for _, f := range fields {
			if f.n > 0 {
				for i := 0; i < f.n; i++ {
					writeField(f.nameLowSnake)
				}
			} else {
				writeField(f.nameLowSnake)
			}
		}
		writeNewLine()
	}

	// 这行开始写入数据
	for i := 0; i < root.Size(); i++ {
		val := root.Get(i)
		for _, f := range fields {
			if f.n > 0 {
				array := val.Get(f.name)

				for i := 0; i < array.Size(); i++ {
					writeField(array.Get(i).ToString())
				}

				for i := array.Size(); i < f.n; i++ {
					writeField("")
				}

			} else {
				// 非数组，直接写入string即可
				if field := val.Get(f.name); field != nil && field.ValueType() != jsoniter.NilValue {
					writeField(field.ToString())
				} else {
					writeField("")
				}
			}
		}
		writeNewLine()
	}

	result = make(map[string][]byte)
	result[path] = b.Bytes()

	return result, nil
}

func toLowerSnakeCase(s string) string {
	var buf bytes.Buffer
	var lastWasUpper bool
	for i, r := range s {
		if unicode.IsUpper(r) && i != 0 && !lastWasUpper {
			buf.WriteRune('_')
		}
		lastWasUpper = unicode.IsUpper(r)
		buf.WriteRune(unicode.ToLower(r))
	}
	return buf.String()
}

func NewKeyValueGameObjects(name, content string, kv ...string) (*GameObjects, error) {

	if name == "" {
		return nil, errors.Errorf("name empty")
	}

	if content == "" {
		return nil, errors.Errorf("content empty")
	}

	gos := &GameObjects{dataMap: map[string][]byte{
		name: []byte(content),
	}}

	n := len(kv)
	if n%2 != 0 {
		return nil, errors.Errorf("len(kv) %2 != 0, kv must be pair, len: %s, %s", len(kv), kv)
	}

	n = n / 2
	for i := 0; i < n; i++ {
		k := kv[i*2]
		if k == "" {
			return nil, errors.Errorf("key empty, index: %s", i*2)
		}

		v := kv[i*2+1]
		if v == "" {
			return nil, errors.Errorf("value empty, index: %s", i*2+1)
		}

		if _, ok := gos.dataMap[k]; ok {
			return nil, errors.Errorf("duplicate key, index: %s, key: %s", i*2, k)
		}

		gos.dataMap[k] = []byte(v)
	}

	return gos, nil
}
