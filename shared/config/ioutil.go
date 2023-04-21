package config

import (
	"bytes"
	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func ReadFile(filename string) ([]byte, error) {
	if confEmbed != nil {
		// 文件不存在, 则读取embed文件
		data, err := confEmbed.ReadFile(filename)
		if err == nil {
			// 从embed中读取（未压缩）
			return data, nil
		}

		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}

		s2filename := filename + ".s2"
		data, err = confEmbed.ReadFile(s2filename)
		if err == nil {
			// 从embed中读取（压缩）
			return decodeS2File(s2filename, data)
		}

		return nil, errors.Wrapf(err, "读文件出错: %s", filename)
	}

	// 文件不存在, 则读取非压缩文件
	data, err := ioutil.ReadFile(filename)
	if err == nil {
		// 从文件中读取（未压缩）
		return data, nil
	}

	if !os.IsNotExist(err) {
		return nil, err
	}

	s2filename := filename + ".s2"
	data, err = ioutil.ReadFile(s2filename)
	if err == nil {
		// 本地文件读取的是压缩文件
		return decodeS2File(s2filename, data)
	}
	return nil, errors.Wrapf(err, "读文件出错: %s", filename)
}

func ReadDir(dir string) ([]fs.FileInfo, error) {
	// 如果存在.s2，那么看下是否存在非s2同名文件，如果存在，则移除，否则包好替换
	fs, err := doReadDir(dir)
	if err != nil {
		return nil, err
	}

	nameMap := make(map[string]struct{})
	for _, f := range fs {
		nameMap[f.Name()] = struct{}{}
	}

	for i := len(fs) - 1; i >= 0; i-- {
		f := fs[i]
		fname := strings.TrimSuffix(f.Name(), ".s2")
		if fname != f.Name() {
			// s2文件
			if _, exist := nameMap[fname]; exist {
				// 存在正常文件，移除s2文件
				fs[i] = fs[len(fs)-1]
				fs = fs[:len(fs)-1]
			} else {
				fs[i] = &s2fileinfo{FileInfo: f, name: fname}
			}
		}
	}
	return fs, nil
}

func doReadDir(dir string) ([]fs.FileInfo, error) {
	if strings.HasSuffix(dir, "/") {
		dir = dir[:len(dir)-1]
	}

	if confEmbed != nil {
		ds, err := confEmbed.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		var fs []fs.FileInfo
		for _, d := range ds {
			f, err := d.Info()
			if err != nil {
				return nil, err
			}
			fs = append(fs, f)
		}
		return fs, nil
	}
	return ioutil.ReadDir(dir)
}

type s2fileinfo struct {
	fs.FileInfo

	name string
}

func (f *s2fileinfo) Name() string {
	return f.name
}

func ReadSmartDir(dir string, exts ...string) (map[string][]byte, error) {

	//有conf中的.s2读conf中的compressed .s2
	//有conf中的非.s2, 则读非压缩的文件
	//
	//没conf读embed
	//有.s2读compressed .s2
	//没有, 读非compressed的文件

	// 先找到符合条件的文件名，再使用ReadSmartFile读取

	nameMap := make(map[string]struct{})
	if fs, err := ioutil.ReadDir(dir); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		for _, v := range fs {
			if !v.IsDir() {
				name := strings.TrimSuffix(v.Name(), ".s2")
				if len(exts) > 0 {
					found := false
					for _, ext := range exts {
						if filepath.Ext(name) == ext {
							found = true
							break
						}
					}

					if !found {
						continue
					}
				}
				nameMap[name] = struct{}{}
			}
		}
	}

	if allEmbed != nil {
		if ds, err := allEmbed.ReadDir(dir); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
		} else {
			for _, d := range ds {
				if !d.IsDir() {
					name := strings.TrimSuffix(d.Name(), ".s2")
					if len(exts) > 0 {
						found := false
						for _, ext := range exts {
							if filepath.Ext(name) == ext {
								found = true
								break
							}
						}

						if !found {
							continue
						}
					}
					nameMap[name] = struct{}{}
				}
			}
		}
	}

	dirWithSep := dir
	if !strings.HasSuffix(dir, "/") {
		dirWithSep = dir + "/"
	}

	dataMap := make(map[string][]byte, len(nameMap))
	for name := range nameMap {
		filename := dirWithSep + name
		data, err := ReadSmartFile(filename)
		if err != nil {
			return nil, errors.Wrapf(err, "读取Smart文件失败, "+filename)
		}
		dataMap[name] = data
	}

	return dataMap, nil
}

func ReadSmartFile(filename string) ([]byte, error) {
	//有conf中的非.s2, 则读非压缩的文件
	//有conf中的.s2读conf中的compressed .s2
	//
	//没conf读embed
	//没有.s2, 读非compressed的文件
	//有.s2读compressed .s2

	// 文件不存在, 则读取非压缩文件
	data, err := ioutil.ReadFile(filename)
	if err == nil {
		// 从文件中读取（未压缩）
		return data, nil
	}

	if !os.IsNotExist(err) {
		return nil, err
	}

	s2filename := filename + ".s2"

	data, err = ioutil.ReadFile(s2filename)
	if err == nil {
		// 本地文件读取的是压缩文件
		return decodeS2File(s2filename, data)
	}

	if !os.IsNotExist(err) {
		return nil, err
	}

	if allEmbed == nil {
		return nil, errors.Wrapf(err, "读文件出错: %s", filename)
	}

	// 文件不存在, 则读取embed文件
	data, err = allEmbed.ReadFile(filename)
	if err == nil {
		// 从embed中读取（未压缩）
		return data, nil
	}

	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	data, err = allEmbed.ReadFile(s2filename)
	if err == nil {
		// 从embed中读取（压缩）
		return decodeS2File(s2filename, data)
	}

	return nil, errors.Wrapf(err, "读文件出错: %s", filename)
}

func ReadS2CompressedFile(filename string) ([]byte, error) {
	var result []byte
	var err error
	if allEmbed != nil {
		result, err = allEmbed.ReadFile(filename)
	} else {
		result, err = ioutil.ReadFile(filename)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "读文件出错: %s", filename)
	}

	return decodeS2File(filename, result)
}

func decodeS2File(filename string, result []byte) ([]byte, error) {

	l, err := s2.DecodedLen(result)
	if err != nil {
		rd := s2.NewReader(bytes.NewReader(result))
		result, err = ioutil.ReadAll(rd)
		if err != nil {
			return nil, errors.Wrapf(err, "解压文件出错: %s", filename)
		}
		return result, err
	} else {
		decompressed := make([]byte, l)
		result, err = s2.Decode(decompressed, result)
		return result, err
	}
}
