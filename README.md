### kafka Consumerのkeyとs3 eq-ftp-datapipeline Bucketの中身の比較を行う

### 1. TopicごとのKey取得
- 以下のコマンドでKafka VMに接続  
` ssh 172.20.250.200 `

- 以下のコマンドで装置ごとのKeyを取得 (引数に取得したい装置番号を記載)
`./keyDownloader.sh F00007 > F00007.csv `  
※ keyDownloader.shは172.20.250.200の/work/keyDownloaderに置いてあるが存在しない場合は配置する必要がある。


### eq-kafka上のデータとKeyの比較を行う

