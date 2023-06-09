package conf_tool

import (
	"Tool-Library/components/filemode"
	"Tool-Library/shared/config"
	"archive/zip"
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

func TransPath(path string) string {
	path = strings.ReplaceAll(path, "\\", "/")
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	return path
}

func ZipXlsxFiles(root string) ([]byte, error) {
	root = TransPath(root)

	fileMap := make(map[string][]byte)
	handleFile := func(root, zipPrefix string, f fs.FileInfo) error {
		if f.IsDir() {
			return nil
		}
		if !strings.HasSuffix(f.Name(), ".xlsx") && !strings.HasSuffix(f.Name(), ".txt") {
			return nil
		}

		if strings.HasPrefix(f.Name(), "~$") {
			return nil
		}

		filename := filepath.Join(root, f.Name())
		data, err := os.ReadFile(filename)
		if err != nil {
			return errors.Wrapf(err, "读取文件失败，file: %s", filename)
		}

		// 压缩文件
		fileMap[zipPrefix+f.Name()] = data
		return nil
	}

	fs, err := config.ReadDir(root)
	if err != nil {
		return nil, errors.Wrapf(err, "读取文件夹失败，root: %s", root)
	}
	for _, f := range fs {
		if err := handleFile(root, "", f); err != nil {
			return nil, err
		}
	}

	zipFile, err := packData(fileMap)
	if err != nil {
		return nil, errors.Wrap(err, "压缩文件失败")
	}

	return zipFile, nil
}

func packData(fileBytes map[string][]byte) ([]byte, error) {

	var ks []string
	for k := range fileBytes {
		ks = append(ks, k)
	}
	sort.Strings(ks)

	// 压缩成zip
	buf := &bytes.Buffer{}
	w := zip.NewWriter(buf)

	for _, k := range ks {
		f, err := w.Create(k)
		if err != nil {
			return nil, err
		}
		_, err = f.Write(fileBytes[k])
		if err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func ZipFiles(root string) ([]byte, error) {

	root = TransPath(root)

	fileMap := make(map[string][]byte)
	handleFile := func(root, zipPrefix string, f fs.FileInfo) error {
		if f.IsDir() {
			return nil
		}

		if strings.HasPrefix(f.Name(), "~$") {
			return nil
		}

		filename := filepath.Join(root, f.Name())
		data, err := os.ReadFile(filename)
		if err != nil {
			return errors.Wrapf(err, "读取文件失败，file: %s", filename)
		}

		// 压缩文件
		fileMap[zipPrefix+f.Name()] = data
		return nil
	}

	fs, err := config.ReadDir(root)
	if err != nil {
		return nil, errors.Wrapf(err, "读取文件夹失败，root: %s", root)
	}
	for _, f := range fs {
		if err := handleFile(root, "", f); err != nil {
			return nil, err
		}
	}

	fmt.Println("压缩路径：", root, "压缩文件数量：", len(fileMap))

	zipFile, err := packData(fileMap)
	if err != nil {
		return nil, errors.Wrap(err, "压缩文件失败")
	}

	return zipFile, nil
}

// return key=filename value=fileBytes
func UnpackData(data []byte) (map[string][]byte, error) {
	buf := bytes.NewReader(data)
	r, err := zip.NewReader(buf, int64(len(data)))
	if err != nil {
		return nil, err
	}

	fileBytes := make(map[string][]byte)
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		b, err := io.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		filename := strings.ReplaceAll(f.Name, "\\", "/")
		fileBytes[filename] = b
	}

	return fileBytes, nil
}

func WriteFile(filename string, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	err := filemode.MkdirAll(path.Dir(filename), 777)
	if err != nil {
		return err
	}

	_, errIsExist := os.Stat(filename)

	if !os.IsNotExist(errIsExist) {
		os.Remove(filename)
	}

	return os.WriteFile(filename, data, 777)
}

func RunCommand(name string, arg ...string) error {

	cmd := exec.Command(name, arg...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout // 标准输出
	cmd.Stderr = &stderr // 标准错误
	err := cmd.Run()

	if err != nil {
		return errors.Wrapf(err, "执行命令失败，name: %s, arg: %v, stdout: %s, stderr: %s", name, arg, stdout.String(), stderr.String())
	}

	outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())

	if len(outStr) > 0 {
		fmt.Println(outStr)
	}

	if len(errStr) > 0 {
		return errors.Errorf(errStr)
	}

	return nil
}
