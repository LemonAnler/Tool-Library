package SqliteDBGen

import (
	"Tool-Library/components/ProtoIDGen"
	"Tool-Library/components/VersionTxtGen"
	conf_tool "Tool-Library/components/conf-tool"
	"Tool-Library/components/filemode"
	"Tool-Library/components/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/tealeg/xlsx"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var starReadLine = 5

var isMarshal = true

var DBVersionName = "DBVersion.json"

func GenerateSqliteDB(confPath string, ProtoPath string, dbGenPathStr string, allDbVersion *[]VersionTxtGen.MsgToDB) error {
	errorMkdir := filemode.MkdirAll(dbGenPathStr, 777)
	if errorMkdir != nil {
		return errors.Errorf("创建genDBPath目录 Err:%v", dbGenPathStr)
	}

	fmt.Println("--------开始加载DBMd5Json--------")

	DBVersionPath := dbGenPathStr + DBVersionName

	DBVersionJson, errDBVersion := os.ReadFile(DBVersionPath)
	if errDBVersion != nil && os.IsNotExist(errDBVersion) {
		//不存在直接创建
		fmt.Println("对应路径下不存在DBVersion，直接创建,路径：", DBVersionPath)
		fp, errCreate := os.Create(DBVersionPath) // 如果文件已存在，会将文件清空。
		if errCreate != nil {
			return errors.Errorf("创建在对应路径下不存在ProtoVersion失败，Err: %v", errCreate)
		}

		DBVersionJson, errDBVersion = os.ReadFile(DBVersionPath)
		if errDBVersion != nil {
			return errors.Errorf("创建在ProtoID记录后，重新读取失败: %v", errDBVersion)
		}
		// defer延迟调用
		defer fp.Close() //关闭文件，释放资源。
	}

	DBVersionData := map[string]map[string]VersionTxtGen.MsgToDB{}

	json.Unmarshal(DBVersionJson, &DBVersionData)

	fmt.Println("--------加载DBMd5Json结束--------")

	fmt.Println("\n--------开始生成数据库--------")

	if strings.HasSuffix(confPath, "/") {
		confPath = confPath[:len(confPath)-1]
	}
	dirWithSep := confPath + "/"

	if fss, errReadDir := os.ReadDir(dirWithSep); errReadDir != nil {
		return errors.Errorf("GenerateProto error 生成失败读取文件, %v", errReadDir)
	} else {
		wg := &sync.WaitGroup{}
		var loadErrorRef atomic.Value

		loadMux := &sync.Mutex{}

		for _, f := range fss {
			fName := f.Name()

			path := dirWithSep + fName
			if filepath.Ext(fName) != ".xlsx" {
				continue
			}

			if strings.Contains(fName, "~$") {
				continue
			}

			wg.Add(1)

			go func() {
				defer wg.Done()
				defer func() {
					if errRecover := recover(); errRecover != nil {
						fmt.Println("GenerateProto error 生成失败,Err:", errRecover)
						debug.PrintStack()
					}
				}()

				loadMux.Lock()
				defer loadMux.Unlock()

				data, errRead := os.ReadFile(path)
				if errRead != nil {
					loadErrorRef.Store(errors.Errorf("GenerateProto error 读取文件失败,path:%v %v", path, errRead))
					return
				}

				excelMd5 := fName + "_" + md5.String(data)

				if _, ok := DBVersionData[excelMd5]; ok {
					//存在已经生成的版本跳过
					excelMd5Proto := DBVersionData[excelMd5]

					for _, db := range excelMd5Proto {
						*allDbVersion = append(*allDbVersion, db)
					}
				} else {
					fmt.Println("生成对应表的DB,表名:", fName)

					DBVersionData[excelMd5] = map[string]VersionTxtGen.MsgToDB{}

					errGen := GenerateTableDB(path, data, ProtoPath, dbGenPathStr, allDbVersion, DBVersionData[excelMd5])

					if errGen != nil {
						loadErrorRef.Store(errors.Errorf("生成数据库失败:%v", errGen))
						return
					}
				}
			}()
		}

		wg.Wait()

		if loadError := loadErrorRef.Load(); loadError != nil {
			return errors.Errorf("多线程生成DB,error, %v", loadError)
		}

		jsonBytes, errJson := json.Marshal(DBVersionData)

		if errJson != nil {
			return errors.Errorf("生成DBVersion JSON失败, %v", errJson)
		}

		if errWrite := conf_tool.WriteFile(DBVersionPath, jsonBytes); errWrite != nil {
			return errors.Errorf("DBVersion 写入JSON失败, %v", errWrite)
		}
	}

	fmt.Println("\n--------生成数据库结束--------")

	return nil
}

func GenerateTableDB(path string, data []byte, ProtoPath string, dbGenPathStr string, allDbVersion *[]VersionTxtGen.MsgToDB, versionDBMap map[string]VersionTxtGen.MsgToDB) error {

	file, errOpenBinary := xlsx.OpenBinary(data)
	if errOpenBinary != nil {
		return errors.Wrapf(errOpenBinary, "解析表格数据失败 OpenBinary 表名：%s", path)
	}

	//获取文件名带后缀
	filenameWithSuffix := filepath.Base(path)
	//获取文件后缀
	fileSuffix := filepath.Ext(path)
	//获取文件名
	filenameOnly := strings.TrimSuffix(filenameWithSuffix, fileSuffix)

	// 找到需要处理sheet（读取sheet(list)）
	ListSheet := file.Sheet["list"]
	if ListSheet == nil {
		return errors.Errorf("表格数据中没有找到list页签 表名：%s", path)
	}

	for i := 0; i < len(ListSheet.Rows); i++ {

		sheetRow := ListSheet.Rows[i]

		if len(sheetRow.Cells) < 1 {
			continue
		}
		sheetName := sheetRow.Cells[0].String()

		curSheet := file.Sheet[strings.TrimSpace(sheetName)]

		if curSheet == nil {
			return errors.Errorf("找不到sheet sheetName  %s in %s", sheetName, path)
		}

		dbName := GetDBTableName(filenameOnly, sheetName, md5.String(data), strconv.Itoa(len(data)))

		_, errIsExist := os.Stat(dbGenPathStr + dbName)

		if !os.IsNotExist(errIsExist) {
			*allDbVersion = append(*allDbVersion, VersionTxtGen.MsgToDB{
				MsgName:   ProtoIDGen.GetMessageName(filenameOnly, sheetName),
				FileName:  dbName,
				TableName: filenameOnly,
				SheetName: sheetName,
			})

			return nil
		}

		messageName := ProtoIDGen.GetMessageName(filenameOnly, sheetName)

		Parser := protoparse.Parser{}
		//加载并解析 proto文件,得到一组 FileDescriptor
		desCs, err := Parser.ParseFiles(ProtoPath + messageName + ".proto")
		if err != nil {
			return errors.Errorf("GenerateSqliteDB error 生成失败解析proto：%v, %v", ProtoPath+messageName+".proto", err)
		}

		fileDescriptor := desCs[0]

		var msgDesc *desc.MessageDescriptor

		for _, v := range fileDescriptor.GetMessageTypes() {
			if messageName == v.GetName() {
				msgDesc = v
			}
		}

		if msgDesc == nil {
			return errors.Errorf("Proto数据中没有找到messageName 消息名称名称：%v", messageName)
		}

		driverConns := []*sqlite3.SQLiteConn{}

		curBackupDriverName := "sqlite3_backup_" + filenameOnly + "_" + sheetName

		sql.Register(curBackupDriverName, &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				driverConns = append(driverConns, conn)
				return nil
			},
		})

		srcDB, errCreateSrcDB := sql.Open(curBackupDriverName, ":memory:")

		if errCreateSrcDB != nil {
			return errors.Errorf("数据库开启失败 err %v", errCreateSrcDB)
		}

		errPing := srcDB.Ping()
		if errPing != nil {
			return errors.Errorf("Failed to connect to the source database:%v", errPing)
		}

		titleRow := curSheet.Rows[1]
		defaultRow := curSheet.Rows[4]
		typeRow := curSheet.Rows[2]

		if len(typeRow.Cells) != len(titleRow.Cells) {
			return errors.Errorf("表格数据类型和标题数量不一致 表名：%s", path)
		}

		for j := starReadLine; j < len(curSheet.Rows); j++ {
			msg := dynamic.NewMessage(msgDesc)

			var keyStr string

			curRow := curSheet.Rows[j]

			isExistKey := false

			for k := 0; k < len(titleRow.Cells); k++ {

				title := titleRow.Cells[k].String()
				strType := typeRow.Cells[k].String()

				if title == "" {
					continue
				}

				if strings.ToLower(title) == "key" || strings.ToLower(title) == "id" {
					isExistKey = true
				}

				cellStr := ""

				if k < len(curRow.Cells) {
					cellStr = curRow.Cells[k].String()
				}

				for _, fieldDesc := range msgDesc.GetFields() {
					fieldName := fieldDesc.GetName()

					if strings.ToLower(fieldName) == strings.ToLower(title) {

						if strings.TrimSpace(cellStr) == "" {
							//为空之后直接去拿默认值,区别Constant 判定是存在key值得表

							if k < len(defaultRow.Cells) && strings.TrimSpace(defaultRow.Cells[k].String()) != "" {
								cellStr = defaultRow.Cells[k].String()
							} else {
								continue
							}
						}

						if strings.HasPrefix(cellStr, "**") {
							continue
						}

						if fieldDesc.GetType().String() == "TYPE_INT32" {

							if fieldDesc.IsRepeated() {

								if len(strings.Split(strType, "_")) == 2 && strings.Split(strType, "_")[1] == "list" {
									valueVec := strings.Split(cellStr, ",")

									for _, value := range valueVec {

										value = strings.TrimSpace(value)

										if value == "" {
											continue
										}

										valueInt, err := strconv.Atoi(value)

										if err != nil {
											return errors.Errorf("表名：%v_%v title:%v type: %v 行数:%d,列数：%d 对应INT数据转换失败：%v ERR:%v", filenameOnly, sheetName, title, strType, j+1, k+1, cellStr, err)
										}

										msg.AddRepeatedFieldByName(fieldDesc.GetName(), int32(valueInt))
									}
								} else {
									value, err := strconv.Atoi(cellStr)

									if err != nil {
										return errors.Errorf("表名：%v_%v 行数:%d,列数：%d 对应INT数据转换失败：%v ERR:%v", filenameOnly, sheetName, j+1, k+1, cellStr, err)
									}

									msg.AddRepeatedFieldByName(fieldDesc.GetName(), int32(value))
								}
							} else {

								value, err := strconv.Atoi(cellStr)

								if err != nil {
									return errors.Errorf("表名：%v_%v 行数:%d,列数：%d 对应INT数据转换失败：%v ERR:%v", filenameOnly, sheetName, j+1, k+1, cellStr, err)
								}

								msg.SetFieldByName(fieldDesc.GetName(), int32(value))
							}

							if strings.ToLower(title) == "id" {
								keyStr = cellStr
							}
						}

						if fieldDesc.GetType().String() == "TYPE_STRING" {

							if fieldDesc.IsRepeated() {
								if len(strings.Split(strType, "_")) == 2 && strings.Split(strType, "_")[1] == "list" {
									valueVec := strings.Split(cellStr, ",")
									for _, value := range valueVec {
										msg.AddRepeatedFieldByName(fieldDesc.GetName(), value)
									}
								} else {
									msg.AddRepeatedFieldByName(fieldDesc.GetName(), cellStr)
								}
							} else {
								msg.SetFieldByName(fieldDesc.GetName(), cellStr)
							}

							if strings.ToLower(title) == "key" || strings.ToLower(title) == "id" {
								keyStr = cellStr
							}
						}

						if fieldDesc.GetType().String() == "TYPE_BOOL" {

							value, err := strconv.ParseBool(cellStr)

							if err != nil {
								return errors.Errorf("行数:%d,列数：%d 对应BOOL数据转换失败：%v ERR:%v", j, k, cellStr, err)
							}

							if fieldDesc.IsRepeated() {
								//布尔不支持重复
							} else {
								msg.SetFieldByName(fieldDesc.GetName(), value)
							}
						}

						if fieldDesc.GetType().String() == "TYPE_FLOAT" {
							if fieldDesc.IsRepeated() {

								if len(strings.Split(strType, "_")) == 2 && strings.Split(strType, "_")[1] == "list" {
									valueVec := strings.Split(cellStr, ",")
									for _, value := range valueVec {
										value = strings.TrimSpace(value)

										if value == "" {
											continue
										}

										valueFloat, err := strconv.ParseFloat(value, 32)

										if err != nil {
											return errors.Errorf("表名：%v_%v 行数:%d,列数：%d 对应FLOAT数据转换失败：%v ERR:%v", filenameOnly, sheetName, j+1, k+1, cellStr, err)
										}

										msg.AddRepeatedFieldByName(fieldDesc.GetName(), float32(valueFloat))
									}
								} else {
									value, err := strconv.ParseFloat(cellStr, 32)

									if err != nil {
										return errors.Errorf("表名：%v_%v 行数:%d,列数：%d 对应FLOAT数据转换失败：%v ERR:%v", filenameOnly, sheetName, j+1, k+1, cellStr, err)
									}

									msg.AddRepeatedFieldByName(fieldDesc.GetName(), float32(value))
								}
							} else {
								cellStr = strings.TrimSpace(cellStr)

								if cellStr == "" {
									continue
								}

								value, err := strconv.ParseFloat(cellStr, 32)

								if err != nil {
									return errors.Errorf("表格数据中FLOAT字段类型错误 %v", err)
								}

								msg.SetFieldByName(fieldDesc.GetName(), float32(value))
							}
						}
						break
					}
				}
			}

			//存在对应的ID 或者key title 字段
			if isExistKey {
				if keyStr == "" || keyStr == "0" {
					continue
				}
			} else {
				//不存在对应的ID 或者key title 字段 基本上就是常量表 拼1个key
				if j == starReadLine {
					keyStr = "1"
				} else {
					continue
				}
			}

			errSaveDB := saveToMemoryDB(srcDB, keyStr, msg)

			if errSaveDB != nil {
				return errors.Errorf("数据库存盘失败 err %v path:%v sheetName:%v", errSaveDB, path, sheetName)
			}
		}

		destDb, errCreateDest := sql.Open(curBackupDriverName, dbGenPathStr+dbName)

		if errCreateDest != nil {
			return errors.Errorf("数据库存盘失败，sql.Open err %v", errCreateDest)
		}

		errPingDest := destDb.Ping()
		if errPingDest != nil {
			return errors.Errorf("Failed to connect to the destination database:%v", errPingDest)
		}

		if errSaveToDB := saveToDB(driverConns, destDb, srcDB, filenameOnly, sheetName); errSaveToDB != nil {
			return errors.Errorf("数据库存盘失败 err %v", errSaveToDB)
		}

		//获取所有消息名字
		newDBInfo := VersionTxtGen.MsgToDB{
			MsgName:   GetMessageName(filenameOnly, sheetName),
			FileName:  dbName,
			TableName: filenameOnly,
			SheetName: sheetName,
		}

		*allDbVersion = append(*allDbVersion, newDBInfo)

		versionDBMap[dbName] = newDBInfo
	}

	return nil
}

func saveToMemoryDB(srcDB *sql.DB, keyStr string, m *dynamic.Message) error {

	if srcDB == nil {
		return errors.Errorf("数据库未开启")
	}

	createTableStr := "CREATE TABLE IF NOT EXISTS data  (id string PRIMARY KEY,data byte[]);"

	_, errCreate := srcDB.Exec(createTableStr)

	if errCreate != nil {
		return errors.Errorf("内存数据库创建表失败，err %v", errCreate)
	}

	var errInsert error

	if isMarshal {
		dataMsg, errMarshal := m.Marshal()

		if errMarshal != nil {
			return errors.Errorf("Marshal err %v", errMarshal)
		}

		_, errInsert = srcDB.Exec("INSERT INTO data (id, data) VALUES (?, ?)", keyStr, dataMsg)
	} else {
		_, errInsert = srcDB.Exec("INSERT INTO data (id, data) VALUES (?, ?)", keyStr, m.String())
	}

	if errInsert != nil {
		return errors.Errorf("内存数据库插入keyStr数据失败,id:%s,err %v data:%v", keyStr, errInsert, m.String())
	}

	return nil
}

func saveToDB(driverConns []*sqlite3.SQLiteConn, destDb *sql.DB, srcDB *sql.DB, tableName string, sheetName string) error {

	if srcDB == nil {
		return errors.Errorf("保存内存数据库")
	}

	if len(driverConns) != 2 {
		return errors.Errorf("Expected 2 driver connections, but found %v.", len(driverConns))
	}

	//开始进行备份
	srcDbDriverConn := driverConns[0]

	if srcDbDriverConn == nil {
		return errors.Errorf("The source database driver connection is nil.")
	}

	destDbDriverConn := driverConns[1]

	if destDbDriverConn == nil {
		return errors.Errorf("The destination database driver connection is nil.")
	}

	// Prepare to perform the backup.
	backup, err := destDbDriverConn.Backup("main", srcDbDriverConn, "main")
	if err != nil {
		return errors.Errorf("Failed to initialize the backup:%v", err)
	}

	// Allow the initial page count and remaining values to be retrieved.
	// According to <https://www.sqlite.org/c3ref/backup_finish.html>, the page count and remaining values are "... only updated by sqlite3_backup_step()."
	isDone, err := backup.Step(0)
	if err != nil {
		return errors.Errorf("Unable to perform an initial 0-page backup step: %v", err)
	}
	if isDone {
		fmt.Println("Backup is unexpectedly done.")
	}

	// Check that the page count and remaining values are reasonable.
	initialPageCount := backup.PageCount()
	if initialPageCount <= 0 {
		fmt.Println("Unexpected initial page count value:", initialPageCount, tableName, sheetName)
		return nil
	}
	initialRemaining := backup.Remaining()
	if initialRemaining <= 0 {
		return errors.Errorf("Unexpected initial remaining value: %v", initialRemaining)
	}
	if initialRemaining != initialPageCount {
		return errors.Errorf("Initial remaining value differs from the initial page count value; remaining: %v; page count: %v", initialRemaining, initialPageCount)
	}

	// Perform the backup.
	if false {
		var startTime = time.Now().Unix()

		// Test backing-up using a page-by-page approach.
		var latestRemaining = initialRemaining
		for {
			// Perform the backup step.
			isDone, err = backup.Step(1)
			if err != nil {
				return errors.Errorf("Failed to perform a backup step:%v", err)
			}

			// The page count should remain unchanged from its initial value.
			currentPageCount := backup.PageCount()
			if currentPageCount != initialPageCount {
				return errors.Errorf("Current page count differs from the initial page count; initial page count: %v; current page count: %v", initialPageCount, currentPageCount)
			}

			// There should now be one less page remaining.
			currentRemaining := backup.Remaining()
			expectedRemaining := latestRemaining - 1
			if currentRemaining != expectedRemaining {
				return errors.Errorf("Unexpected remaining value; expected remaining value: %v; actual remaining value: %v", expectedRemaining, currentRemaining)
			}
			latestRemaining = currentRemaining

			if isDone {
				break
			}

			// Limit the runtime of the backup attempt.
			if (time.Now().Unix() - startTime) > 150 {
				return errors.Errorf("Backup is taking longer than expected.")
			}
		}
	} else {
		// Test the copying of all remaining pages.
		isDone, err = backup.Step(-1)
		if err != nil {
			return errors.Errorf("Failed to perform a backup step:%v", err)
		}
		if !isDone {
			return errors.Errorf("Backup is unexpectedly not done.")
		}
	}

	// Finish the backup.
	err = backup.Finish()
	if err != nil {
		return errors.Errorf("Failed to finish backup:%v", err)
	}

	destDb.Close()
	srcDB.Close()

	return nil
}

func GetDBTableName(filenameOnly string, sheetName string, param1 string, param2 string) string {

	return filenameOnly + "_" + sheetName + "_" + param1 + param2 + ".db"
}

func GetMessageName(filenameOnly string, sheetName string) string {
	return "confpb" + filenameOnly + sheetName
}
