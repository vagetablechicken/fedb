For full instructions, please refer to [website page](http://openmldb.ai/docs/zh/main/use_case/JD_recommendation.html) or [github md](https://github.com/4paradigm/OpenMLDB/blob/main/docs/zh/use_case/JD_recommendation.md)

# Pre
1. env var `demodir`: the dir of demo source


1. Oneflow Env: use conda



# Training

## OpenMLDB Feature Extraction

```
docker run -dit --name=openmldb --network=host -v $demodir:/work/oneflow_demo 4pdosc/openmldb:0.6.0 bash
docker exec -it openmldb bash
```

in docker:

1. start cluster and create tables, load offline data
```
/work/init.sh

/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/create_tables.sql

/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/load_offline_data.sql
```

2. waiting for load jobs done
```
echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

3. feature extraction
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/sync_select_out.sql
```
The feature data will be saved in `/work/oneflow_demo/out/1` in docker container file system.

## Oneflow Train

Use conda env:
```
conda env create -n oneflow python=3.9.2
pip install --pre oneflow -f https://staging.oneflow.info/branch/support_oneembedding_serving/cu102
pip install psutil petastorm pandas sklearn xxhash tritonclient geventhttpclient tornado


```

1. Launch oneflow deepfm model training:
```
cd $demodir/feature_preprocess/
python preprocess.py $demodir/out/1
```

#Model Serving
1. Configure openmldb for online feature extraction:
##in openmldb docker
docker exec -it demo bash
##deploy feature extraction
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/deploy.sql
##load online data
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/load_online_data.sql

2. Configure oneflow for model serving
#check if config.pbtxt, model files, persistent path are correcly set

3. Start prediction server
cd openmldb_serving/
## start prediction server
./start_predict_server.sh 0.0.0.0:9080
## start oneflow serving
replace demodir with your demo folder path
docker run --runtime=nvidia --rm --network=host \
  -v $demodir/oneflow_process/model:/models \
  -v /home/gtest/work/oneflow_serving/serving/build/libtriton_oneflow.so:/backends/oneflow/libtriton_oneflow.so \
  -v /home/gtest/work/oneflow_serving/oneflow/build/liboneflow_cpp/lib/:/mylib \
  -v $demodir/oneflow_process/persistent:/root/demo/persistent \
  registry.cn-beijing.aliyuncs.com/oneflow/triton-devel \
  bash -c 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mylib /opt/tritonserver/bin/tritonserver \
  --model-repository=/models --backend-directory=/backends'

Test:
## send data for prediciton
python predict.py
