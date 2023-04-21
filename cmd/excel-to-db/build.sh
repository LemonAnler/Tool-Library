rm gen/exceltodb*.zip

go build -o exceltodb.exe ./cmd/excel-to-db/main.go

cp -rf exceltodb.exe  bin/

time=$(date "+%Y%m%d-%H%M%S")

buildName=exceltodb$time

mkdir -p gen/$buildName

mv exceltodb.exe gen/$buildName

cp -rf conf gen/$buildName/

cp ./cmd/excel-to-db/run.bat  gen/$buildName/

zip -qr gen/$buildName.zip gen/$buildName/

rm -rf gen/$buildName