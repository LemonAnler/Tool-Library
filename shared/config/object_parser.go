package config

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type ObjectParser struct {
	dataMap map[string][]string

	filename string
	line     int
}

func (p *ObjectParser) Line() string {
	return fmt.Sprintf("%s:%d", p.filename, p.line)
}

func (p *ObjectParser) String(key string) string {
	sa := p.OriginStringArray(key)

	if len(sa) > 0 {
		return replaceNewLine(sa[0])
	}

	return ""
}

func replaceNewLine(s string) string {
	s = strings.Replace(s, "\\n", "\n", -1)
	s = strings.Replace(s, "\\r", "\r", -1)
	return s
}

func (p *ObjectParser) Int32(key string) int32 {
	s := p.String(key)
	if len(s) == 0 {
		return 0
	}

	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}

	return int32(i)
}

func (p *ObjectParser) Int(key string) int {
	s := p.String(key)
	if len(s) == 0 {
		return 0
	}

	i, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}

	return i
}

func (p *ObjectParser) Int64(key string) int64 {
	s := p.String(key)
	if len(s) == 0 {
		return 0
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}

	return i
}

func (p *ObjectParser) Uint64(key string) uint64 {
	s := p.String(key)
	if len(s) == 0 {
		return 0
	}

	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}

	return i
}

func (p *ObjectParser) Float64(key string) float64 {
	s := p.String(key)
	if len(s) == 0 {
		return 0
	}

	i, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}

	return i
}

func (p *ObjectParser) Bool(key string) bool {
	s := p.String(key)
	if len(s) == 0 {
		return false
	}

	i, err := strconv.ParseBool(s)
	if err != nil {
		return false
	}

	return i
}

func (p *ObjectParser) KeyExist(key string) bool {
	_, ok := p.dataMap[strings.ToLower(key)]
	return ok
}

func (p *ObjectParser) OriginStringArray(key string) []string {
	return p.dataMap[strings.ToLower(key)]
}

func (p *ObjectParser) IntArray(key, sep string, nullable bool) []int {
	sa := p.StringArray(key, sep, nullable)

	out := make([]int, 0, len(sa))
	for _, s := range sa {

		v, err := strconv.Atoi(s)
		if err != nil {
			logrus.Errorf("配置解析错误(之前不是检查过类型吗...)，IntArray %s %s, %s", key, sep, sa)

			out = append(out, -1)
			continue
		}

		out = append(out, v)
	}

	return out
}

func (p *ObjectParser) Int32Array(key, sep string, nullable bool) []int32 {
	sa := p.StringArray(key, sep, nullable)

	out := make([]int32, 0, len(sa))
	for _, s := range sa {

		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			logrus.Errorf("配置解析错误(之前不是检查过类型吗...)，Int32Array %s %s, %s", key, sep, sa)

			out = append(out, -1)
			continue
		}

		out = append(out, int32(v))
	}

	return out
}

func (p *ObjectParser) Int64Array(key, sep string, nullable bool) []int64 {
	sa := p.StringArray(key, sep, nullable)

	out := make([]int64, 0, len(sa))
	for _, s := range sa {

		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			logrus.Errorf("配置解析错误(之前不是检查过类型吗...)，Int64Array %s %s, %s", key, sep, sa)

			out = append(out, -1)
			continue
		}

		out = append(out, v)
	}

	return out
}

func (p *ObjectParser) Uint64Array(key, sep string, nullable bool) []uint64 {
	sa := p.StringArray(key, sep, nullable)

	out := make([]uint64, 0, len(sa))
	for _, s := range sa {

		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			logrus.Errorf("配置解析错误(之前不是检查过类型吗...)，Uint64Array %s %s, %s", key, sep, sa)

			out = append(out, 0)
			continue
		}

		out = append(out, v)
	}

	return out
}

func (p *ObjectParser) Float64Array(key, sep string, nullable bool) []float64 {
	sa := p.StringArray(key, sep, nullable)

	out := make([]float64, 0, len(sa))
	for _, s := range sa {

		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			logrus.Errorf("配置解析错误(之前不是检查过类型吗...)，Float64Array %s %s, %s", key, sep, sa)

			out = append(out, -1)
			continue
		}

		out = append(out, v)
	}

	return out
}

func (p *ObjectParser) BoolArray(key, sep string, nullable bool) []bool {
	sa := p.StringArray(key, sep, nullable)

	out := make([]bool, 0, len(sa))
	for _, s := range sa {

		v, err := strconv.ParseBool(s)
		if err != nil {
			logrus.Errorf("配置解析错误(之前不是检查过类型吗...)，BoolArray %s %s, %s", key, sep, sa)

			out = append(out, false)
			continue
		}

		out = append(out, v)
	}

	return out
}

func (p *ObjectParser) StringArray(key, sep string, nullable bool) []string {
	in := p.OriginStringArray(key)
	if len(in) == 0 {
		return nil
	}

	out := in
	if sep != "" {
		if len(in) > 1 {
			logrus.Errorf("配置解析错误(之前不是检查过类型吗...)，StringArray len(in) > 1, %s, %s", in, sep)
		}

		out = strings.Split(in[0], sep)
	}

	if nullable {
		return out
	}

	newOut := make([]string, 0, len(out))
	for _, v := range out {
		if len(v) > 0 {
			newOut = append(newOut, replaceNewLine(v))
		}
	}

	return newOut
}

func strArr2IntArr(in []string) ([]int, error) {

	out := make([]int, len(in))
	for i, s := range in {
		v, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Wrapf(err, "strArr2IntArr atoi error, %s", s)
		}

		out[i] = v
	}

	return out, nil
}

func NewObjectParser(heads, fields []string, filename string, line int) *ObjectParser {
	p := &ObjectParser{
		dataMap:  make(map[string][]string),
		filename: filename,
		line:     line,
	}

	for j := 0; j < len(heads); j++ {
		var v string
		if j < len(fields) {
			fields[j] = strings.TrimSpace(fields[j])
			fields[j] = strings.Trim(fields[j], "\"")
			v = fields[j]
		}

		key := strings.ToLower(heads[j])
		arr := p.dataMap[key]
		if key == "column" {
			if len(v) <= 0 {
				continue
			}

			index := strings.Index(v, "#")
			if index == -1 {
				panic(fmt.Sprintf("初始化 ObjectParser 时，column的配置格式非法，没有配置#进行分割: %v, %v, %d", heads, arr, line))
			}

			key := strings.ToLower(v[:index])
			arr := p.dataMap[key]
			if len(arr) == 0 {
				p.dataMap[key] = []string{v[index+1:]}
			} else {
				p.dataMap[key] = append(arr, v[index+1:])
			}
		} else {
			if len(arr) == 0 {
				p.dataMap[key] = []string{v}
			} else {
				p.dataMap[key] = append(arr, v)
			}
		}
	}

	return p
}

func ParseList(filename, fileContent string) ([]*ObjectParser, error) {
	list, _, err := ParseListWithHeads(filename, fileContent)
	return list, err
}

func ParseListWithHeads(filename, fileContent string) ([]*ObjectParser, []string, error) {

	if len(fileContent) == 0 {
		return make([]*ObjectParser, 0), nil, nil
	}

	fileContent = deleteHeadRN(fileContent)

	as := strings.Split(fileContent, "\r\n")
	if len(as) <= 1 {
		as = strings.Split(fileContent, "\n")
		if len(as) <= 1 {
			as = strings.Split(fileContent, "\r")
			if len(as) <= 1 {
				return nil, nil, errors.Errorf("%s 格式不正确，请复制一个正常文件，然后使用excel来编辑保存", filename)
			}
		}
	}

	heads := strings.Split(as[1], "\t")

	for i := 0; i < len(heads); i++ {
		heads[i] = strings.TrimSpace(heads[i])
		heads[i] = strings.Trim(heads[i], "\"")
	}

	parsers := make([]*ObjectParser, 0)

	for i := 2; i < len(as); i++ {
		if len(strings.TrimSpace(as[i])) == 0 {
			// empty line
			continue
		}

		if strings.HasPrefix(as[i], "**") {
			// 注释行，过滤掉
			continue
		}

		line := i + 1
		fields := strings.Split(as[i], "\t")
		if len(heads) < len(fields) {
			fields = fields[:len(heads)]
		}

		parsers = append(parsers, NewObjectParser(heads, fields, filename, line))
	}

	return parsers, heads, nil
}

func ParseKv(filename, fileContent string) (*ObjectParser, error) {

	if len(fileContent) == 0 {
		return nil, nil
	}

	fileContent = deleteHeadRN(fileContent)

	as := strings.Split(fileContent, "\r\n")
	if len(as) <= 1 {
		as = strings.Split(fileContent, "\n")
		if len(as) <= 1 {
			as = strings.Split(fileContent, "\r")
			if len(as) <= 1 {
				return nil, errors.Errorf("%s 格式不正确，请复制一个正常文件，然后使用excel来编辑保存", filename)
			}
		}
	}

	var heads []string
	var fields []string

	for _, value := range as[1:] {
		if len(strings.TrimSpace(value)) == 0 {
			// empty line
			continue
		}

		if strings.HasPrefix(value, "**") {
			// 注释行，过滤掉
			continue
		}

		// 分隔
		keyValue := strings.Split(value, "\t")
		if len(keyValue) < 2 {
			return nil, errors.Errorf("%s 格式不正确，必须要配置一个key，一个value", filename)
		}

		heads = append(heads, strings.TrimSpace(keyValue[0]))
		fields = append(fields, strings.TrimSpace(keyValue[1]))
	}

	return NewObjectParser(heads, fields, filename, 0), nil
}

func deleteHeadRN(origin string) string {
	// 将双引号中间的换行符删掉
	array := strings.Split(origin, "\"")
	if len(array) <= 1 {
		return origin
	}

	buf := bytes.Buffer{}

	for i := 0; i < len(array); i++ {
		s := array[i]
		if len(s) == 0 {
			continue
		}

		if i%2 == 1 {
			s = strings.Replace(s, "\r", "", -1)
			s = strings.Replace(s, "\n", "", -1)
		}

		if i > 0 {
			buf.WriteString("\"")
		}

		buf.WriteString(s)
	}

	return buf.String()

}
