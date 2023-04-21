package SqliteDBGen

import (
	"Tool-Library/components/VersionTxtGen"
	"Tool-Library/components/filemode"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
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

func GenerateSqliteDB(confPath string, confProtoPath string, dbGenPathStr string, allDbVersion *[]VersionTxtGen.MsgToDB) error {

	errorMkdir := filemode.MkdirAll(dbGenPathStr, 777)
	if errorMkdir != nil {
		return errors.Errorf("创建genDBPath目录 Err:%v", dbGenPathStr)
	}

	fmt.Println("\n--------开始生成数据库--------")

	Parser := protoparse.Parser{}
	//加载并解析 proto文件,得到一组 FileDescriptor
	desCs, err := Parser.ParseFiles(confProtoPath)
	if err != nil {
		return errors.Errorf("GenerateSqliteDB error 生成失败解析proto, %v", err)
	}

	fileDescriptor := desCs[0]

	if strings.HasSuffix(confPath, "/") {
		confPath = confPath[:len(confPath)-1]
	}
	dirWithSep := confPath + "/"

	if fss, errReadDir := os.ReadDir(dirWithSep); errReadDir != nil {
		return errors.Errorf("GenerateProto error 生成失败读取文件, %v", errReadDir)
	} else {
		fmap := make(map[string]struct{})
		for _, f := range fss {
			fName := f.Name()

			path := dirWithSep + fName
			if filepath.Ext(fName) != ".xlsx" {
				continue
			}

			if strings.Contains(fName, "~$") {
				continue
			}

			fmap[path] = struct{}{}
		}

		fmt.Print("\n表格数量：", len(fmap), "\n")

		wg := &sync.WaitGroup{}
		var loadErrorRef atomic.Value

		loadMux := &sync.Mutex{}

		for k := range fmap {

			path := k
			wg.Add(1)

			go func() {
				defer wg.Done()
				defer func() {
					if errRecover := recover(); errRecover != nil {
						fmt.Println("GenerateProto error 生成失败,Err:", errRecover)
						debug.PrintStack()
					}
				}()

				data, errRead := os.ReadFile(path)
				if errRead != nil {
					loadErrorRef.Store(errors.Errorf("GenerateProto error 读取文件失败,path:%v %v", path, errRead))
					return
				}

				errGen := GenerateTableDB(path, data, fileDescriptor, dbGenPathStr, allDbVersion)

				if errGen != nil {
					loadErrorRef.Store(errors.Errorf("生成数据库失败:%v", errGen))
					return
				}

				loadMux.Lock()
				defer loadMux.Unlock()
			}()
		}

		wg.Wait()

		if loadError := loadErrorRef.Load(); loadError != nil {
			return errors.Errorf("多线程生成DB,error, %v", loadError)
		}

	}

	fmt.Println("\n--------生成数据库结束--------")

	return nil
}

func GenerateTableDB(path string, data []byte, fileDescriptor *desc.FileDescriptor, dbGenPathStr string, allDbVersion *[]VersionTxtGen.MsgToDB) error {
	file, errOpenBinary := xlsx.OpenBinary(data)
	if errOpenBinary != nil {
		return errors.Wrapf(errOpenBinary, "解析表格数据失败 OpenBinary 表名：%s", path)
	}

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

		filenameOnly, msgDesc, errMsgGet := getMsgDesc(path, sheetName, fileDescriptor)

		if errMsgGet != nil {
			return errors.Errorf("获取表名字和消息结果：%v", errMsgGet)
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

		for j := starReadLine; j < len(curSheet.Rows); j++ {
			msg := dynamic.NewMessage(msgDesc)

			var keyStr string

			curRow := curSheet.Rows[j]

			isExistKey := false

			for k := 0; k < len(curRow.Cells); k++ {

				if k >= len(titleRow.Cells) {
					continue
				}

				title := titleRow.Cells[k].String()

				if title == "" {
					continue
				}

				if strings.ToLower(title) == "key" || strings.ToLower(title) == "id" {
					isExistKey = true
				}

				curCell := curRow.Cells[k]

				for _, fieldDesc := range msgDesc.GetFields() {
					fieldName := fieldDesc.GetName()

					if strings.ToLower(fieldName) == strings.ToLower(title) {

						cellStr := curCell.String()

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

							value, err := strconv.Atoi(cellStr)

							if err != nil {
								return errors.Errorf("表名：%v_%v 行数:%d,列数：%d 对应INT数据转换失败：%v ERR:%v", filenameOnly, sheetName, j+1, k+1, curCell.String(), err)
							}

							if fieldDesc.IsRepeated() {
								msg.AddRepeatedFieldByName(fieldDesc.GetName(), int32(value))
							} else {
								msg.SetFieldByName(fieldDesc.GetName(), int32(value))
							}

							if strings.ToLower(title) == "id" {
								keyStr = strconv.Itoa(value)
							}
						}

						if fieldDesc.GetType().String() == "TYPE_STRING" {

							value := cellStr

							if fieldDesc.IsRepeated() {
								msg.AddRepeatedFieldByName(fieldDesc.GetName(), value)
							} else {
								msg.SetFieldByName(fieldDesc.GetName(), value)
							}

							if strings.ToLower(title) == "key" || strings.ToLower(title) == "id" {
								keyStr = value
							}
						}

						if fieldDesc.GetType().String() == "TYPE_BOOL" {

							value, err := strconv.ParseBool(cellStr)

							if err != nil {
								return errors.Errorf("行数:%d,列数：%d 对应BOOL数据转换失败：%v ERR:%v", j, k, curCell.String(), err)
							}

							if fieldDesc.IsRepeated() {

							} else {
								msg.SetFieldByName(fieldDesc.GetName(), value)
							}
						}

						if fieldDesc.GetType().String() == "TYPE_FLOAT" {

							value, err := strconv.ParseFloat(cellStr, 32)

							if err != nil {
								return errors.Errorf("表格数据中FLOAT字段类型错误 %v", err)
							}

							if fieldDesc.IsRepeated() {
								msg.AddRepeatedFieldByName(fieldDesc.GetName(), float32(value))
							} else {
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

		dbName := GetDBTableName(filenameOnly, sheetName)

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
		*allDbVersion = append(*allDbVersion, VersionTxtGen.MsgToDB{
			MsgName:   GetMessageName(filenameOnly, sheetName),
			FileName:  ReDBName(dbGenPathStr + dbName),
			TableName: filenameOnly,
			SheetName: sheetName,
		})
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

func GetDBTableName(filenameOnly string, sheetName string) string {
	return filenameOnly + "_" + sheetName + ".db"
}

func GetMessageName(filenameOnly string, sheetName string) string {
	return "confpb" + filenameOnly + sheetName
}

func getMsgDesc(path string, sheetName string, fileDescriptor *desc.FileDescriptor) (string, *desc.MessageDescriptor, error) {
	//获取文件名带后缀
	filenameWithSuffix := filepath.Base(path)
	//获取文件后缀
	fileSuffix := filepath.Ext(path)
	//获取文件名
	filenameOnly := strings.TrimSuffix(filenameWithSuffix, fileSuffix)

	messageName := GetMessageName(filenameOnly, sheetName)

	var msgDesc *desc.MessageDescriptor

	for _, v := range fileDescriptor.GetMessageTypes() {
		if messageName == v.GetName() {
			msgDesc = v
		}
	}

	if msgDesc == nil {
		return "", nil, errors.Errorf("表格数据中没有找到messageName 消息名称名称：%v", messageName)
	}

	return filenameOnly, msgDesc, nil
}

func SqliteTest(confPath string, genDBPath string, confProtoPath string, dbName string) error {

	fmt.Println("开启测试:", dbName)

	Parser := protoparse.Parser{}
	//加载并解析 proto文件,得到一组 FileDescriptor
	desCs, err := Parser.ParseFiles(confProtoPath)
	if err != nil {
		return errors.Errorf("GenerateSqliteDB error 生成失败解析proto, %v", err)
	}

	fileDescriptor := desCs[0]

	xlsx_sheet := strings.Split(dbName, ".")[0]

	tableName := strings.Split(xlsx_sheet, "_")[0]

	sheetName := strings.Split(xlsx_sheet, "_")[1]

	_, msgDesc, errMsgGet := getMsgDesc(tableName+".xlsx", sheetName, fileDescriptor)

	if errMsgGet != nil {
		return errors.Errorf("test getMsgDesc Err:", errMsgGet)
	}

	testDb, err := sql.Open("sqlite3", genDBPath+dbName)

	if err != nil {
		return errors.Errorf("打开数据库失败，err:%v", err)
	}

	errPing := testDb.Ping()
	if errPing != nil {
		return errors.Errorf("连接数据库失败，err:%v", errPing)
	}

	rows, err := testDb.Query("SELECT id,data FROM data")

	if err != nil {
		return errors.Errorf("查询数据库失败，err:%v", err)
	}

	curMessage := dynamic.NewMessage(msgDesc)

	for rows.Next() {
		var id string
		var data []byte
		rows.Scan(&id, &data)

		errUnmarshal := curMessage.Unmarshal(data)

		if errUnmarshal != nil {
			return errors.Errorf("Unmarshal err %v", errUnmarshal)
		}

		fmt.Println("id:", id, "data:", curMessage.String())
	}

	return nil
}

func ReDBName(dBPath string) string {

	//获取文件名带后缀
	filenameWithSuffix := filepath.Base(dBPath)
	//获取文件后缀
	fileSuffix := filepath.Ext(dBPath)
	//获取文件名
	filenameOnly := strings.TrimSuffix(filenameWithSuffix, fileSuffix)

	path := strings.TrimSuffix(dBPath, filenameWithSuffix)

	bytes, errRead := os.ReadFile(dBPath)
	if errRead != nil {
		fmt.Printf("读取文件失败 表名:%v", filenameOnly)
		return ""
	}

	h := md5.New()

	h.Write(bytes)

	cipherStr := h.Sum(nil)

	newName := filenameOnly + "_" + hex.EncodeToString(cipherStr) + "_" + strconv.Itoa(len(bytes)) + fileSuffix

	errRename := os.Rename(dBPath, path+newName)
	if errRename != nil {
		return ""
	}

	return newName
}
