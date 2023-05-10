package main

import (
	"Tool-Library/components/SqliteDBGen"
	"fmt"
)

func main() {
	err := SqliteDBGen.GetDBStr("HeroMain_Data_92b3c0b64f15529a1cd22829447678c035743.db", "confpbHeroMainData")

	if err != nil {
		fmt.Println("err:", err)
	}
}
