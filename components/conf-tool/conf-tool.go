package conf_tool

import (
	"Tool-Library/components/filemode"
	"Tool-Library/shared/config"
	"archive/zip"
	"bufio"
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
	"sync"
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

	cmd.Stdin = os.Stdin

	var wg sync.WaitGroup
	wg.Add(2)
	//捕获标准输出
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Errorf("捕获标准输出 ERROR:%v", err)
	}
	readout := bufio.NewReader(stdout)
	go func() {
		defer wg.Done()
		GetOutput(readout)
	}()

	//捕获标准错误
	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Println("ERROR:", err)
		os.Exit(1)
	}
	readErr := bufio.NewReader(stderr)

	errCmdOut := ""
	go func() {
		defer wg.Done()
		errCmdOut = GetOutput(readErr)
	}()

	//执行命令
	cmd.Run()
	wg.Wait()

	if errCmdOut != "" {
		return errors.Errorf("执行命令失败，err: %s \n", errCmdOut)
	}

	return nil
}

func GetOutput(reader *bufio.Reader) string {
	var sumOutput string //统计屏幕的全部输出内容
	outputBytes := make([]byte, 200)
	for {
		n, err := reader.Read(outputBytes) //获取屏幕的实时输出(并不是按照回车分割，所以要结合sumOutput)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err)
			sumOutput += err.Error()
		}
		output := string(outputBytes[:n])
		fmt.Print(output) //输出屏幕内容
		sumOutput += output
	}
	return sumOutput
}
