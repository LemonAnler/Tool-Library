package config

import (
	"fmt"
	"github.com/axgle/mahonia"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tealeg/xlsx"
	unicode16 "golang.org/x/text/encoding/unicode"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

func NewStarGameObjects(dir string) (*GameObjects, error) {
	loadMux := &sync.Mutex{}
	var loadErrorRef atomic.Value

	tsvMap := make(map[string][]byte)
	fileMap := make(map[string]*starfile)
	wg := &sync.WaitGroup{}

	if strings.HasSuffix(dir, "/") {
		dir = dir[:len(dir)-1]
	}
	dirWithSep := dir + "/"

	if fss, err := ReadDir(dir); err != nil {
		return nil, err
	} else {
		fmap := make(map[string]struct{})
		for _, f := range fss {
			fname := f.Name()
			path := dirWithSep + fname
			if filepath.Ext(fname) != ".xlsx" {
				if filepath.Ext(fname) == ".tsv" {
					data, err := ReadFile(path)
					if err != nil {
						return nil, err
					}
					tsvMap[path] = data
				}
				continue
			}

			if strings.Contains(fname, "~$") {
				continue
			}

			fmap[path] = struct{}{}
		}

		for k := range fmap {
			path := k

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if err := recover(); err != nil {
						logrus.WithField("err", err).Errorf("newStarEmbedGameObjects panic recover")
						loadErrorRef.Store(errors.Errorf("newStarEmbedGameObjects error, %v", err))
						debug.PrintStack()
					}
				}()
				data, err := ReadFile(path)
				if err != nil {
					logrus.WithField("err", err).Errorf("open xlsx file %s", path)
					if loadErrorRef.Load() == nil {
						loadErrorRef.Store(errors.Wrapf(err, "open xlsx file %s", path))
					}
					return
				}

				file, err := parseStarXlsx(path, data)
				if err != nil {
					logrus.WithField("err", err).Errorf("newStarEmbedGameObjects, %v", path)
					if errors.Cause(err) == listSheetNotFound {
						return
					}

					if loadErrorRef.Load() == nil {
						loadErrorRef.Store(errors.Wrapf(err, "parseStarXlsx fail %s", path))
					}
					return
				}

				loadMux.Lock()
				defer loadMux.Unlock()
				fileMap[file.name] = file
			}()
		}
	}
	wg.Wait()

	if loadError := loadErrorRef.Load(); loadError != nil {
		return nil, loadError.(error)
	}

	gos, err := parseStarFiles(fileMap)
	if err != nil {
		return nil, err
	}

	// 处理tsv
	if err := parseTsv(gos, tsvMap); err != nil {
		return nil, err
	}

	return gos, nil
}

func parseStarFiles(fileMap map[string]*starfile) (*GameObjects, error) {

	// 处理filter的替换
	matchMap := make(map[string]map[string]string)
	for _, file := range fileMap {
		name := strings.TrimSuffix(file.name, ".xlsx")
		for _, sheet := range file.sheets {
			if len(sheet.matchMap) > 0 {
				key := strings.ToLower(name + sheet.sheet.Name)
				matchMap[key] = sheet.matchMap
			}
		}
	}

	// 转换GameObjects格式
	gos := &GameObjects{}
	gos.dataMap = make(map[string][]byte)
	gos.gbkDecoder = mahonia.NewDecoder("gbk")

	loadMux := &sync.Mutex{}
	var loadErrorRef atomic.Value
	wg := &sync.WaitGroup{}
	for _, f := range fileMap {
		file := f
		for _, s := range file.sheets {
			starSheet := s
			if len(starSheet.serverIndex) <= 0 {
				// 服务器不需要这个表
				continue
			}

			sheet := starSheet.sheet
			//for _, matchNames := range starSheet.filterMap {
			//	for _, matchName := range matchNames {
			//		match := matchMap[matchName]
			//		if match == nil {
			//			return nil, errors.Errorf("filter_string(%v) not found, file: %v sheet: %v", matchName, file.name, sheet.Name)
			//		}
			//	}
			//}

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if err := recover(); err != nil {
						logrus.WithField("err", err).Errorf("parseStarFiles error, %v", err)
						loadErrorRef.Store(errors.Errorf("parseStarFiles error, %v", err))
						debug.PrintStack()
					}
				}()

				sb := strings.Builder{}

				// 2行表头
				headRow := sheet.Rows[1]
				for _, idx := range starSheet.serverIndex {
					if idx >= len(headRow.Cells) {
						sb.WriteString("")
						sb.WriteString("\t")
						continue
					}

					cell := headRow.Cells[idx]
					text, err := getCellString(cell)
					if err != nil {
						logrus.WithError(err).Errorf("getCellString fail, %s", cell.String())
						if loadErrorRef.Load() == nil {
							loadErrorRef.Store(errors.Wrapf(err, "读取表头[%v] getCellString fail %s", idx+1, cell))
						}
						return
					}

					// 特殊转换，ID -> Id
					text = strings.ReplaceAll(text, "ID", "Id")

					sb.WriteString(toLowerSnakeCase(text))
					sb.WriteString("\t")
				}
				sb.WriteString("\r\n")
				sb.WriteString(sb.String()) // 第二行表头

				for i := 5; i < len(sheet.Rows); i++ {
					row := sheet.Rows[i]

					if isEmptyRow(row, starSheet) {
						continue
					}

					for i, idx := range starSheet.serverIndex {

						def := starSheet.defVal[i]
						if idx >= len(row.Cells) {
							sb.WriteString(def)
							sb.WriteString("\t")
							continue
						}
						cell := row.Cells[idx]

						text, err := getCellString(cell)
						if err != nil {
							logrus.WithError(err).Errorf("getCellString fail, %s", cell.String())
							if loadErrorRef.Load() == nil {
								loadErrorRef.Store(errors.Wrapf(err, "读取表头[%v] getCellString fail %s", idx+1, cell))
							}
							return
						}

						if matchNames, exist := starSheet.filterMap[idx]; exist {

							// {haha}_1_{hehe}_2
							newValue, err := replaceMatchString(text, matchMap, matchNames)
							if err != nil {
								logrus.WithError(err).Errorf("%s/%s filter_string替换失败 %d行-%d列 %v, matchNames: %v", file.name, sheet.Name, i+1, idx+1, text, matchNames)
								if loadErrorRef.Load() == nil {
									loadErrorRef.Store(errors.Wrapf(err, "%s/%s filter_string替换失败 %d行-%d列 %v, matchNames: %v", file.name, sheet.Name, i+1, idx+1, text, matchNames))
								}
								return
							}
							text = newValue
						}

						if text == "" {
							text = def
						}

						sb.WriteString(text)
						sb.WriteString("\t")
					}
					sb.WriteString("\r\n")
				}

				filename := file.name + ":" + sheet.Name

				loadMux.Lock()
				defer loadMux.Unlock()
				if _, exist := gos.dataMap[filename]; exist {
					logrus.Errorf("%s/%s 文件重复", file.name, sheet.Name)
					if loadErrorRef.Load() == nil {
						loadErrorRef.Store(errors.Errorf("%s/%s 文件重复", file.name, sheet.Name))
					}
				}

				gos.dataMap[filename] = []byte(sb.String())
			}()
		}
	}

	wg.Wait()

	if loadError := loadErrorRef.Load(); loadError != nil {
		return nil, loadError.(error)
	}

	return gos, nil
}

func isEmptyRow(row *xlsx.Row, starSheet *starsheet) bool {
	for _, idx := range starSheet.serverIndex {
		if idx >= len(row.Cells) {
			break
		}
		cell := row.Cells[idx]

		text, err := getCellString(cell)
		if err != nil {
			return false
		}

		if text != "" {
			return false
		}
	}
	return true
}

type starfile struct {
	name string

	file *xlsx.File

	sheets []*starsheet
}

type starsheet struct {
	sheet *xlsx.Sheet

	serverIndex []int

	// 默认值
	defVal []string

	// key=match_text value=match_int
	matchMap map[string]string

	// key=columnIndex value=filterPath
	filterMap map[int][]string
}

var listSheetNotFound = errors.New("没找到list页签")

func parseStarXlsx(path string, data []byte) (*starfile, error) {

	file, err := xlsx.OpenBinary(data)
	if err != nil {
		return nil, errors.Wrapf(err, "parse xlsx file %s", path)
	}

	// 找到需要处理sheet（读取sheet(list)）
	sheet := file.Sheet["list"]
	if sheet == nil {
		for _, s := range file.Sheets {
			if strings.ToLower(s.Name) == "list" {
				sheet = s
				break
			}
		}
		if sheet == nil {
			return nil, listSheetNotFound
		}
	}

	var sheetNames []string
	for _, row := range sheet.Rows {
		if len(row.Cells) <= 0 {
			continue
		}

		name := strings.TrimSpace(row.Cells[0].String())
		if name != "" {
			sheetNames = append(sheetNames, name)
		}
	}

	if len(sheetNames) <= 0 {
		return nil, errors.Errorf("list页签中没有配置数据 in %s", path)
	}

	// 检查sheet是否都存在
	var sheets []*starsheet
	for _, name := range sheetNames {
		sheet := file.Sheet[name]
		if sheet == nil {
			return nil, errors.Errorf("sheet配置在list中，但是这个sheet不存在 %s in %s", name, path)
		}

		if len(sheet.Rows) < 5 {
			return nil, errors.Errorf("sheet行数太少（%v） %s in %s", len(sheet.Rows), name, path)
		}

		exportRow := sheet.Rows[3]
		defRow := sheet.Rows[4]
		var serverIndex []int
		var defVal []string
		for i, cell := range exportRow.Cells {
			val, err := getCellString(cell)
			if err != nil {
				return nil, errors.Wrapf(err, "读取cell失败")
			}

			val = strings.ToLower(strings.TrimSpace(val))
			if val == "c" || val == "s" || val == "cs" {
				serverIndex = append(serverIndex, i)

				dd := ""
				if i < len(defRow.Cells) {
					def, err := getCellString(defRow.Cells[i])
					if err != nil {
						return nil, errors.Wrapf(err, "读取cell失败")
					}
					dd = strings.TrimSpace(def)
				}

				defVal = append(defVal, dd)
			}
		}

		starSheet := &starsheet{
			sheet:       sheet,
			serverIndex: serverIndex,
			defVal:      defVal,
			matchMap:    make(map[string]string),
			filterMap:   make(map[int][]string),
		}

		sheets = append(sheets, starSheet)
	}

	f := &starfile{}
	f.name = filepath.Base(path)
	f.file = file
	f.sheets = sheets

	//name := strings.TrimSuffix(f.name, ".xlsx")

	for _, starSheet := range sheets {
		sheet := starSheet.sheet
		//nameRow := sheet.Rows[0]
		typeRow := sheet.Rows[2]

		// 找到match_int 和 match_string|match_text 字段

		matchIntIndex := -1
		matchTextIndex := -1

		r := 3
		for i, cell := range typeRow.Cells {
			c := i + 1

			text, err := getCellString(cell)
			if err != nil {
				return nil, errors.Wrapf(err, "%s 解析失败 %d行-%d列 %v", sheet.Name, r, c, cell)
			}

			if text == "match_int" {
				if matchIntIndex != -1 {
					return nil, errors.Errorf("%s 解析失败 发现有多个match_int字段, %v, %v", sheet.Name, matchIntIndex+1, i+1)
				}
				matchIntIndex = i
			} else if text == "match_text" {
				if matchTextIndex != -1 {
					return nil, errors.Errorf("%s 解析失败 发现有多个match_text字段, %v, %v", sheet.Name, matchTextIndex+1, i+1)
				}
				matchTextIndex = i
			} else {
				str := strings.TrimPrefix(text, "filter_string(")
				if str == text {
					str = strings.TrimPrefix(text, "filter_int(")
				}

				if str != text {
					str = strings.TrimSuffix(str, ")")

					arr := strings.Split(str, ",")
					for _, v := range arr {
						starSheet.filterMap[i] = append(starSheet.filterMap[i], strings.ToLower(strings.TrimSpace(v)))
					}
				}
			}
		}

		if matchIntIndex != -1 {
			if matchTextIndex != -1 {
				// 遍历获取到所有的映射关系（从第六行开始遍历）
				for i := 5; i < len(sheet.Rows); i++ {
					row := sheet.Rows[i]

					if matchIntIndex < len(row.Cells) && matchTextIndex < len(row.Cells) {
						intCell := row.Cells[matchIntIndex]
						intValue, err := getCellString(intCell)
						if err != nil {
							return nil, errors.Wrapf(err, "%s 解析失败 %d行-%d列 %v", sheet.Name, i+1, matchIntIndex+1, intCell)
						}

						textCell := row.Cells[matchTextIndex]
						textValue, err := getCellString(textCell)
						if err != nil {
							return nil, errors.Wrapf(err, "%s 解析失败 %d行-%d列 %v", sheet.Name, i+1, matchTextIndex+1, textCell)
						}

						intValue = strings.TrimSpace(intValue)
						textValue = strings.TrimSpace(textValue)

						if intValue == "" {
							if textValue != "" {
								return nil, errors.Wrapf(err, "%s 解析失败(match_int empty) %d行-%d列 %v", sheet.Name, i+1, matchTextIndex+1, textCell)
							}
							// 都是空，跳过
							continue
						} else if textValue == "" {
							return nil, errors.Wrapf(err, "%s 解析失败(match_text empty) %d行-%d列 %v", sheet.Name, i+1, matchTextIndex+1, textCell)
						}

						if _, exist := starSheet.matchMap[textValue]; exist {
							return nil, errors.Errorf("%s 解析 %d行-%d列 %v, 发现重复的match_text", sheet.Name, i+1, matchTextIndex+1, textCell)
						}

						starSheet.matchMap[textValue] = intValue
					}

				}

			} else {
				return nil, errors.Errorf("%s 解析失败 存在match_int但是不存在match_text字段, %v, %v", sheet.Name, matchIntIndex+1, matchTextIndex+1)
			}

		} else if matchTextIndex != -1 {
			return nil, errors.Errorf("%s 解析失败 不存在match_int但是存在match_text字段, %v, %v", sheet.Name, matchIntIndex+1, matchTextIndex+1)
		}
	}

	return f, nil
}

func getCellString(cell *xlsx.Cell) (string, error) {
	text := cell.Value

	var err error
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

	if err == nil {
		// 用2个空格替换1个tab
		text = strings.ReplaceAll(text, "\t", "  ")

		// 去掉双引号
		text = strings.ReplaceAll(text, "\"", "")
	}

	return text, err
}

func replaceMatchString(s string, matchDataMap map[string]map[string]string, matchNames []string) (string, error) {
	// {haha}_1_{hehe}_2

	array := strings.Split(s, "{")
	if len(array) <= 1 {
		return s, nil
	}

	sb := strings.Builder{}
	sb.WriteString(array[0])

	for i := 1; i < len(array); i++ {
		arr := strings.Split(array[i], "}")
		if len(arr) != 2 {
			return "", errors.Errorf("替换filter字段失败，{}必须成对出现, %v", s)
		}

		// 这个key只能在一个match中找到，多个不行，打死
		var foundMatchNames []string
		var matchValue string
		for _, matchName := range matchNames {
			matchMap := matchDataMap[matchName]
			val := matchMap[arr[0]]
			if val != "" {
				foundMatchNames = append(foundMatchNames, matchName)
				matchValue = val
			}
		}

		if len(foundMatchNames) <= 0 {
			return "", errors.Errorf("替换filter字段失败，%v，替换类型没找到，%v", s, arr[0])
		}

		if len(foundMatchNames) > 1 {
			return "", errors.Errorf("替换filter字段失败，%v，替换内容[%v]在多个match_text中找到，%v", s, arr[0], foundMatchNames)
		}

		sb.WriteString(matchValue)
		sb.WriteString(arr[1])
	}

	return sb.String(), nil
}

func parseTsv(gos *GameObjects, tsvMap map[string][]byte) error {
	if len(tsvMap) <= 0 {
		return nil
	}

	utf16leDecoder := unicode16.UTF16(unicode16.LittleEndian, unicode16.UseBOM).NewDecoder()
	utf16beDecoder := unicode16.UTF16(unicode16.BigEndian, unicode16.UseBOM).NewDecoder()

	for path, data := range tsvMap {
		dp := filepath.Base(path)

		if len(data) >= 2 {

			src := data

			switch {
			case src[0] == 0xfe && src[1] == 0xff:
				data, err := utf16beDecoder.Bytes(src)
				if err != nil {
					return errors.Wrapf(err, "解析utf16be文件出错: %s", path)
				}
				gos.dataMap[dp] = data
				continue
			case src[0] == 0xff && src[1] == 0xfe:
				data, err := utf16leDecoder.Bytes(src)
				if err != nil {
					return errors.Wrapf(err, "解析utf16le文件出错: %s", path)
				}
				gos.dataMap[dp] = data
				continue
			default:
			}
		}

		// 字符编码转换
		if !utf8.Valid(data) {
			gos.dataMap[dp] = []byte(gos.gbkDecoder.ConvertString(string(data)))
		} else {
			gos.dataMap[dp] = data
		}
	}

	return nil
}
