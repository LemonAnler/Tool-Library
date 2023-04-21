package main

import (
	conf_tool "Tool-Library/components/conf-tool"
	"flag"
	"os"
)

// filepath: 要编译的文件的路径
func build(buildName string, filepath string) {
	_ = os.Setenv("CGO_ENABLED", "0")
	_ = os.Setenv("GOARCH", "amd64")
	_ = os.Setenv("GOOS", "linux")

	conf_tool.RunCommand("go", "build", "-o", buildName, filepath)
}

var exeName = flag.String("name", "main.exe", "编译后的文件名")

var filePath = flag.String("path", "./cmd/conf-http/main.go", "要编译的文件的路径")

func main() {
	flag.Parse()

	build(*exeName, *filePath) //要编译的文件的路径
}

//go build -o /d/SystemPath/golinux.exe ./cmd/build/build.go
