dos2unix ./cmd/excel-to-db/build.sh && chmod +x ./cmd/excel-to-db/build.sh 

./cmd/excel-to-db/build.sh

dos2unix ./cmd/cs-gen/build.sh && chmod +x ./cmd/cs-gen/build.sh 

./cmd/cs-gen/build.sh

go build ./cmd/conf-http/main.go
