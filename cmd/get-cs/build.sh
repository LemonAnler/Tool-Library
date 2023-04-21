rm gen/getcs*.zip

go build -o getcs.exe ./Cmd/get-cs/main.go

cp -rf getcs.exe  bin/

time=$(date "+%Y%m%d-%H%M%S")

buildName=getcs$time

mkdir -p gen/$buildName

mv getcs.exe gen/$buildName

cp -rf conf gen/$buildName/

cp ./cmd/get-cs/run.bat  gen/$buildName/

zip -qr gen/$buildName.zip gen/$buildName/

rm -rf gen/$buildName