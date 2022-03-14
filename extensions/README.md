pip install matplotlib pandas sklearn lightgbm openmldb

openmldb release package
sed -i s/:2181/:6181/g conf/* # any port you want

openmldb python sdk(dbapi/sqlalchemy) needs:
1. **block load data to offline**: 
给rpc多加一个field，不用config那个，bool 是否阻塞，默认false
session variable传阻塞配置，rpc发task manager前配置下rpc msg的阻塞field
taskmanager收到后，一样异步发，但是轮询是否成功，结束后回复。做成阻塞的样子。
2. set/show variable
3. use db


data prepare:
https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview

kaggle competitions download -c talkingdata-adtracking-fraud-detection
unzip to ./extensions/data 
