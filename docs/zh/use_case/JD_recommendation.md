
#  OpenMLDB + OneFlow: 高潜用户购买意向预测

本文我们将以[京东高潜用户购买意向预测问题](https://jdata.jd.com/html/detail.html?id=1)为例，示范如何使用[OpenMLDB](https://github.com/4paradigm/OpenMLDB)和 [OneFlow](https://github.com/Oneflow-Inc/oneflow) 联合来打造一个完整的机器学习应用。

如何从历史数据中找出规律，去预测用户未来的购买需求，让最合适的商品遇见最需要的人，是大数据应用在精准营销中的关键问题，也是所有电商平台在做智能化升级时所需要的核心技术。京东作为中国最大的自营式电商，沉淀了数亿的忠实用户，积累了海量的真实数据。本案例以京东商城真实的用户、商品和行为数据（脱敏后）为基础，通过数据挖掘的技术和机器学习的算法，构建用户购买商品的预测模型，输出高潜用户和目标商品的匹配结果，为精准营销提供高质量的目标群体，挖掘数据背后潜在的意义，为电商用户提供更简单、快捷、省心的购物体验。本案例使用OpenMLDB进行数据挖掘，使用OneFlow中的[DeepFM](https://github.com/Oneflow-Inc/models/tree/main/RecommenderSystems/deepfm)模型进行高性能训练推理，提供精准的商品推荐。全量数据[下载链接](https://openmldb.ai/download/jd-recommendation/JD_data.tgz)。

本案例基于 OpenMLDB 集群版进行教程演示。注意，本文档使用的是演示用的 docker 镜像。如果要在生产环境中使用，需要进行生产环境的OpenMLDB集群部署。

## 1.  环境准备和预备知识

### 1.1 下载demo演示用的数据与脚本

下载demo演示用的数据与脚本，在后面的步骤中可以直接使用。
todo demo中应该有这个文件夹，不用去哪儿下载。
http://openmldb.ai/download/jd-recommendation/demo.tgz 或者checkout最新代码，`demo/jd-recommendation`。

我们将脚本目录定为环境变量`demodir`，之后的脚本中多会使用这一环境变量。所以，你需要配置这一变量：
```
export demodir=<your_path>/demo
```

我们仅使用小数据集做演示。如果你想要使用全量数据集，请下载[JD_data](http://openmldb.ai/download/jd-recommendation/JD_data.tgz)。

### 1.2 OneFlow工具包安装
OneFlow工具依赖GPU的强大算力，所以请确保部署机器具备Nvidia GPU，并且保证驱动版本 >=460.X.X  [驱动版本需支持CUDA 11.0](https://docs.nvidia.com/cuda/cuda-toolkit-release-notes/index.html#cuda-major-component-versions)。
我们推荐使用conda来管理oneflow环境，使用以下指令创建默认python3，并安装OneFlow，以及本案例演示所需的依赖：
```bash
conda env create -n oneflow python=3.9.2 -y
pip install --pre oneflow -f https://staging.oneflow.info/branch/support_oneembedding_serving/cu102
pip install psutil petastorm pandas sklearn xxhash tritonclient geventhttpclient tornado
```
或使用todo environment.yml

### 1.3 启动 OpenMLDB Docker 容器
- 注意，请确保 Docker Engine 版本号 >= 18.03

为了快速运行OpenMLDB集群，我们推荐使用镜像启动的方式。由于OpenMLDB集群需要和其他组件网络通信，我们直接使用host网络。并且，我们将在容器中使用已下载的脚本，所以请将数据脚本所在目录`demodir`映射为容器中的目录：

```bash
docker run -dit --name=openmldb --network=host -v $demodir:/work/oneflow_demo 4pdosc/openmldb:0.6.0 bash
docker exec -it openmldb bash
```
- 上述镜像预装了OpenMLDB的工具等，我们需要进一步安装OneFlow推理所需依赖。

todo: 下面的是否需要装在openmldb容器里？放oneflow，pyspark？

因为我们将在OpenMLDB的容器中嵌入OneFlow模型推理的预处理及调用，需要安装以下的依赖。
```bash
pip install tritonclient geventhttpclient
```

```{note}
注意，本教程以下的OpenMLDB部分的演示命令默认均在 1.3 启动的 docker 容器`openmldb`内运行。OneFlow命令默认在 1.2 安装的OneFlow虚拟环境`oneflow`下运行。
```

### 1.4 创建OpenMLDB集群

```bash
./init.sh
```
我们在镜像内提供了init.sh脚本帮助用户快速创建集群。

### 1.5 使用 OpenMLDB CLI

操作OpenMLDB集群，我们使用OpenMLDB CLI，使用命令如下：
```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```
```{note}
注意，本教程大部分命令在 OpenMLDB CLI 下执行，为了跟普通 shell 环境做区分，在 OpenMLDB CLI 下执行的命令均使用特殊的提示符  `>`  。
```

```{important}
在OpenMLDB集群版中，使用离线引擎的操作，默认都是非阻塞任务，包括在本次演示中将会用到的`LOAD  DATA`（离线/在线模式均使用离线引擎），和离线`SELECT  INTO`命令。

任务提交以后，可以使用[`SHOW JOBS`](../reference/sql/task_manage/SHOW_JOBS.md),  [`SHOW JOB <job_id>`](../reference/sql/task_manage/SHOW_JOB.md)来查看任务进度。
```

## 2. 机器学习训练流程
### 2.1 流程概览
使用OpenMLDB+OneFlow进行机器学习训练可概括为:
1. OpenMLDB离线特征设计与抽取（SQL)
1. OneFlow模型训练
1. SQL和模型上线
接下来会介绍每一个步骤的具体操作细节。

### 2.2 使用OpenMLDB进行离线特征抽取
#### 2.2.1 创建数据库和数据表
以下命令均在 OpenMLDB CLI 中执行。
```sql
> CREATE DATABASE JD_db;
> USE JD_db;
> CREATE TABLE action(reqId string, eventTime timestamp, ingestionTime timestamp, actionValue int);
> CREATE TABLE flattenRequest(reqId string, eventTime timestamp, main_id string, pair_id string, user_id string, sku_id string, time bigint, split_id int, time1 string);
> CREATE TABLE bo_user(ingestionTime timestamp, user_id string, age string, sex string, user_lv_cd string, user_reg_tm bigint);
> CREATE TABLE bo_action(ingestionTime timestamp, pair_id string, time bigint, model_id string, type string, cate string, br string);
> CREATE TABLE bo_product(ingestionTime timestamp, sku_id string, a1 string, a2 string, a3 string, cate string, br string);
> CREATE TABLE bo_comment(ingestionTime timestamp, dt bigint, sku_id string, comment_num int, has_bad_comment string, bad_comment_rate float);
```
也可使用sql脚本(`/work/oneflow_demo/sql_scripts/create_tables.sql`)直接运行：

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/create_tables.sql
```
#### 2.2.2 离线数据导入

我们需要将源数据导入到OpenMLDB中作为离线数据，用于离线特征计算。

如果你导入较大的数据集，可以使用软链接的方式，减少导入消耗时间。本次演示中仅导入很少的数据，因此硬拷贝也不会消耗太多时间。

```sql
> USE JD_db;
> SET @@execute_mode='offline';
> LOAD DATA INFILE '/work/oneflow_demo/data/action/*.parquet' INTO TABLE action options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/work/oneflow_demo/data/flattenRequest_clean/*.parquet' INTO TABLE flattenRequest options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_user/*.parquet' INTO TABLE bo_user options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_action/*.parquet' INTO TABLE bo_action options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_product/*.parquet' INTO TABLE bo_product options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_comment/*.parquet' INTO TABLE bo_comment options(format='parquet', header=true, mode='overwrite');
```
或使用脚本执行，并快速查询jobs状态：

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/load_offline_data.sql

echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```
```{important}
请等待`SHOW JOBS`中所有任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```

#### 2.2.3 特征设计

通常在设计特征前，用户需要根据机器学习的目标对数据进行分析，然后根据分析设计和调研特征。机器学习的数据分析和特征研究不是本文讨论的范畴，我们将不作展开。本文假定用户具备机器学习的基本理论知识，有解决机器学习问题的能力，能够理解SQL语法，并能够使用SQL语法构建特征。针对本案例，用户经过分析和调研设计了若干特征。

请注意，在实际的机器学习特征调研过程中，科学家对特征进行反复试验，寻求模型效果最好的特征集。所以会不断的重复多次特征设计->离线特征抽取->模型训练过程，并不断调整特征以达到预期效果。

#### 2.2.4 离线特征抽取

源数据准备好以后，就可以进行离线特征抽取，我们将特征结果输出到`'/work/oneflow_demo/out/1'`目录下保存（对应映射为`$demodir/out/1`，方便容器外部使用特征数据），以供后续的模型训练。 `SELECT` 命令对应了基于上述特征设计所产生的 SQL 特征计算脚本。
```sql
> USE JD_db;
> select * from
(
select
    `reqId` as reqId_1,
    `eventTime` as flattenRequest_eventTime_original_0,
    `reqId` as flattenRequest_reqId_original_1,
    `pair_id` as flattenRequest_pair_id_original_24,
    `sku_id` as flattenRequest_sku_id_original_25,
    `user_id` as flattenRequest_user_id_original_26,
    distinct_count(`pair_id`) over flattenRequest_user_id_eventTime_0_10_ as flattenRequest_pair_id_window_unique_count_27,
    fz_top1_ratio(`pair_id`) over flattenRequest_user_id_eventTime_0_10_ as flattenRequest_pair_id_window_top1_ratio_28,
    fz_top1_ratio(`pair_id`) over flattenRequest_user_id_eventTime_0s_14d_200 as flattenRequest_pair_id_window_top1_ratio_29,
    distinct_count(`pair_id`) over flattenRequest_user_id_eventTime_0s_14d_200 as flattenRequest_pair_id_window_unique_count_32,
    case when !isnull(at(`pair_id`, 0)) over flattenRequest_user_id_eventTime_0_10_ then count_where(`pair_id`, `pair_id` = at(`pair_id`, 0)) over flattenRequest_user_id_eventTime_0_10_ else null end as flattenRequest_pair_id_window_count_35,
    dayofweek(timestamp(`eventTime`)) as flattenRequest_eventTime_dayofweek_41,
    case when 1 < dayofweek(timestamp(`eventTime`)) and dayofweek(timestamp(`eventTime`)) < 7 then 1 else 0 end as flattenRequest_eventTime_isweekday_43
from
    `flattenRequest`
    window flattenRequest_user_id_eventTime_0_10_ as (partition by `user_id` order by `eventTime` rows between 10 preceding and 0 preceding),
    flattenRequest_user_id_eventTime_0s_14d_200 as (partition by `user_id` order by `eventTime` rows_range between 14d preceding and 0s preceding MAXSIZE 200))
as out0
last join
(
select
    `flattenRequest`.`reqId` as reqId_3,
    `action_reqId`.`actionValue` as action_actionValue_multi_direct_2,
    `bo_product_sku_id`.`a1` as bo_product_a1_multi_direct_3,
    `bo_product_sku_id`.`a2` as bo_product_a2_multi_direct_4,
    `bo_product_sku_id`.`a3` as bo_product_a3_multi_direct_5,
    `bo_product_sku_id`.`br` as bo_product_br_multi_direct_6,
    `bo_product_sku_id`.`cate` as bo_product_cate_multi_direct_7,
    `bo_product_sku_id`.`ingestionTime` as bo_product_ingestionTime_multi_direct_8,
    `bo_user_user_id`.`age` as bo_user_age_multi_direct_9,
    `bo_user_user_id`.`ingestionTime` as bo_user_ingestionTime_multi_direct_10,
    `bo_user_user_id`.`sex` as bo_user_sex_multi_direct_11,
    `bo_user_user_id`.`user_lv_cd` as bo_user_user_lv_cd_multi_direct_12
from
    `flattenRequest`
    last join `action` as `action_reqId` on `flattenRequest`.`reqId` = `action_reqId`.`reqId`
    last join `bo_product` as `bo_product_sku_id` on `flattenRequest`.`sku_id` = `bo_product_sku_id`.`sku_id`
    last join `bo_user` as `bo_user_user_id` on `flattenRequest`.`user_id` = `bo_user_user_id`.`user_id`)
as out1
on out0.reqId_1 = out1.reqId_3
last join
(
select
    `reqId` as reqId_14,
    max(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_bad_comment_rate_multi_max_13,
    min(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0_10_ as bo_comment_bad_comment_rate_multi_min_14,
    min(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_bad_comment_rate_multi_min_15,
    distinct_count(`comment_num`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_comment_num_multi_unique_count_22,
    distinct_count(`has_bad_comment`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_has_bad_comment_multi_unique_count_23,
    fz_topn_frequency(`has_bad_comment`, 3) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_has_bad_comment_multi_top3frequency_30,
    fz_topn_frequency(`comment_num`, 3) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_comment_num_multi_top3frequency_33
from
    (select `eventTime` as `ingestionTime`, bigint(0) as `dt`, `sku_id` as `sku_id`, int(0) as `comment_num`, '' as `has_bad_comment`, float(0) as `bad_comment_rate`, reqId from `flattenRequest`)
    window bo_comment_sku_id_ingestionTime_0s_64d_100 as (
UNION (select `ingestionTime`, `dt`, `sku_id`, `comment_num`, `has_bad_comment`, `bad_comment_rate`, '' as reqId from `bo_comment`) partition by `sku_id` order by `ingestionTime` rows_range between 64d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_comment_sku_id_ingestionTime_0_10_ as (
UNION (select `ingestionTime`, `dt`, `sku_id`, `comment_num`, `has_bad_comment`, `bad_comment_rate`, '' as reqId from `bo_comment`) partition by `sku_id` order by `ingestionTime` rows between 10 preceding and 0 preceding INSTANCE_NOT_IN_WINDOW))
as out2
on out0.reqId_1 = out2.reqId_14
last join
(
select
    `reqId` as reqId_17,
    fz_topn_frequency(`br`, 3) over bo_action_pair_id_ingestionTime_0s_10h_100 as bo_action_br_multi_top3frequency_16,
    fz_topn_frequency(`cate`, 3) over bo_action_pair_id_ingestionTime_0s_10h_100 as bo_action_cate_multi_top3frequency_17,
    fz_topn_frequency(`model_id`, 3) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_model_id_multi_top3frequency_18,
    distinct_count(`model_id`) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_model_id_multi_unique_count_19,
    distinct_count(`model_id`) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_model_id_multi_unique_count_20,
    distinct_count(`type`) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_type_multi_unique_count_21,
    fz_topn_frequency(`type`, 3) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_type_multi_top3frequency_40,
    fz_topn_frequency(`type`, 3) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_type_multi_top3frequency_42
from
    (select `eventTime` as `ingestionTime`, `pair_id` as `pair_id`, bigint(0) as `time`, '' as `model_id`, '' as `type`, '' as `cate`, '' as `br`, reqId from `flattenRequest`)
    window bo_action_pair_id_ingestionTime_0s_10h_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 10h preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_action_pair_id_ingestionTime_0s_7d_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 7d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_action_pair_id_ingestionTime_0s_14d_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 14d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out3
on out0.reqId_1 = out3.reqId_17
INTO OUTFILE '/work/oneflow_demo/out/1';
```
```{note}
注意，集群版 `SELECT INTO` 为非阻塞任务，可以使用命令 `SHOW JOBS` 查看任务运行状态，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。耗时大概1分半。
```

因为这里只有一个特征抽取任务，可以使用阻塞的运行方式，命令完成即特征抽取完成。直接运行sql脚本`sync_select_out.sql`:
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/sync_select_out.sql
```

### 2.3 预处理特征数据以配合DeepFM模型要求

```{note}
注意，以下命令在docker外执行，使用安装了1.2所描述的OneFlow运行环境
```
根据 [DeepFM 论文](https://arxiv.org/abs/1703.04247), 类别特征和连续特征都被当作稀疏特征对待。

> χ may include categorical fields (e.g., gender, location) and continuous fields (e.g., age). Each categorical field is represented as a vector of one-hot encoding, and each continuous field is represented as the value itself, or a vector of one-hot encoding after discretization.

进入demodir文件夹，并使用预处理脚本做特征数据预处理。运行需要pandas,xxhash等依赖，推荐在`oneflow`虚拟环境中运行。
```bash
cd $demodir/feature_preprocess/
python preprocess.py $demodir/out/1
```

脚本将在 `$demodir/feature_preprocess/out`对应生成train，test，valid三个parquet数据集，并将三者的行数和`table_size_array`保存在文件`data_info.txt`中。运行结果打印类似：
```
feature total count: 13916
train count: 11132
saved to <demodir>/feature_preprocess/out/train
test count: 1391
saved to <demodir>/feature_preprocess/out/test
val count: 1393
saved to <demodir>/feature_preprocess/out/valid
table size array:
 4,26,16,4,11,809,1,1,5,3,17,16,7,13916,13890,13916,10000,3674,9119,7,2,13916,5,4,4,33,2,2,7,2580,3,5,13916,10,47,13916,365,17,132,32,37
saved to <demodir>/feature_preprocess/out/data_info.txt
```
得到的文件结构类似：
```
out/
├── data_info.txt
├── test
│   └── test.parquet
├── train
│   └── train.parquet
└── valid
    └── valid.parquet

3 directories, 4 files
```

### 2.4 启动OneFlow进行模型训练
```{note}
注意，以下命令在安装1.2所描述的OneFlow运行环境中运行
```
```bash
cd $demodir/oneflow_process/
sh train_deepfm.sh -h
Usage: train_deepfm.sh DATA_DIR(abs)
        We'll read required args in $DATA_DIR/data_info.txt, and save results in path ./
```
OneFLow模型训练使用该目录中的`train_deepfm.sh`脚本，使用说明如上所示。通常情况下，我们不用特别配置。脚本会自动读取`$DATA_DIR/data_info.txt`的参数，包括`num_train_samples`,`num_val_samples`,`num_test_samples`和`table_size_array`。此处，我们应该使用预处理后的特征数据集（绝对路径），命令如下：
```bash
bash train_deepfm.sh $demodir/feature_preprocess/out
```
生成模型将存放在`$demodir/oneflow_process/model_out`，用来serving的模型存放在`$demodir/oneflow_process/model/embedding/1/model`。

```{note}
请勿更改训练模型的保存路径，因为我们在`$demodir/oneflow_process/model/embedding`中提前准备好了`config.pbtxt`用于serving。
```

## 3. 模型上线流程
### 3.1 流程概览
使用OpenMLDB+OneFlow进行模型serving，主要步骤为：
1. OpenMLDB上线： SQL上线，准备在线数据
1. Oneflow上线：加载模型
1. 启动预测服务，我们使用一个简单的predict server做展示

接下来会介绍每一个步骤的具体操作细节。

### 3.2 OpenMLDB上线

#### 3.2.1 特征抽取SQL脚本上线

假定离线训练效果理想，我们就可以将该特征抽取SQL脚本上线，提供实时在线的特征抽取服务。

1. 重新启动 OpenMLDB CLI。
   ```bash
   docker exec -it demo bash
   /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
   ```
2. 执行上线部署，在 OpenMLDB CLI 中deploy 离线特征抽取使用的SQL（SQL较长，见[离线特征抽取](#224-离线特征抽取)，此处不展示SQL）。
```sql
> USE JD_db;
> DEPLOY demo <SQL>;
```
也可以在docker容器内直接运行：
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/deploy.sql
```

可使用如下命令可确认deploy信息:
```sql
show deployment demo;
```
deploy后，可通过访问OpenMLDB ApiServer `127.0.0.1:9080`进行实时特征计算。

#### 3.2.2 在线数据准备

在**在线**执行模式下，导入在线数据，用于在线特征计算。为了简单起见，我们直接导入和离线一致的数据集。生产中，通常是离线是大量的冷数据，在线是近期的热数据。

以下命令均在 OpenMLDB CLI 下执行。
```sql
> USE JD_db;
> SET @@execute_mode='online';
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/action/*.parquet' INTO TABLE action options(format='parquet', mode='append');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/flattenRequest_clean/*.parquet' INTO TABLE flattenRequest options(format='parquet', mode='append');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_user/*.parquet' INTO TABLE bo_user options(format='parquet', mode='append');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_action/*.parquet' INTO TABLE bo_action options(format='parquet', mode='append');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_product/*.parquet' INTO TABLE bo_product options(format='parquet', mode='append');
> LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_comment/*.parquet' INTO TABLE bo_comment options(format='parquet', mode='append');
```
也可以使用：
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/load_online_data.sql
```

使用以下命令快速查看jobs的状态：
```
 echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

```{note}
注意，在线 `LOAD  DATA` 也是非阻塞任务，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。

如果你导入全量数据，那么请开启spark并发线程，否则在线导入速度将会较慢。
```

### 3.3 配置OneFlow推理服务

OneFlow的推理服务需要[OneEmbedding](https://docs.oneflow.org/master/cookies/one_embedding.html)的支持。该支持目前还没有合入主框架中。若需要重新编译，可参考附录A进行编译测试。接下来步骤默认相关支持已编译完成，并且存放在`$demodir/oneflow_serving/`路径中。todo有点大，提供下载？1.7G说一下用了哪些库，编译的话在哪个位置

#### 3.3.1 检查

1. 模型路径（`$demodir/oneflow_process/model`）结果是否如下所示。
```
$ tree  -L 3 model/
model/
└── embedding
    ├── 1
    │   └── model
    └── config.pbtxt
 ```   
1. 修改`config.pbtxt`中的`one_embedding_persistent_table_path`配置。请填入正确的persistent绝对路径（`$demodir/oneflow_process/persistent/0-1`），不是使用环境变量。
 
 #### 3.3.2 启动OneFLow推理服务

 使用以下命令启动OneFlow推理服务：
```bash
docker run --runtime=nvidia --name triton-serving -d --network=host \
  -v $demodir/oneflow_process/model:/models \
  -v $demodir/oneflow_serving/libtriton_oneflow.so:/backends/oneflow/libtriton_oneflow.so \
  -v $demodir/oneflow_serving/lib/:/mylib \
  -v $demodir/oneflow_process/persistent:/root/demo/persistent \
  registry.cn-beijing.aliyuncs.com/oneflow/triton-devel \
  bash -c 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mylib /opt/tritonserver/bin/tritonserver \
  --model-repository=/models --backend-directory=/backends'
```
启动后，可访问`http://127.0.0.1:8000`进行推理。


### 3.4 启动推理服务

```{note}
以下命令可在物理机环境执行，由于Python依赖，推荐OneFlow虚拟环境中执行。
```
脚本中参数使用`127.0.0.1:9080`作为OpenMLDB ApiServer地址，`127.0.0.1:8000`作为OneFlow Triton地址。
```bash
cd $demodir/serving
./start_predict_server.sh todo
```

### 3.5 发送预估请求
预估请求可在OpenMLDB的容器外执行。容器外部访问的具体信息可参见[IP 配置](https://openmldb.ai/docs/zh/main/reference/ip_tips.html)。
在普通命令行下执行内置的 `predict.py` 脚本。该脚本发送一行请求数据到预估服务，接收返回的预估结果，并打印出来。
```bash
python $demodir/serving/predict.py
```
范例输出：
```
----------------ins---------------
['200001_80005_2016-03-31 18:11:20' 1459419080000
 '200001_80005_2016-03-31 18:11:20' '200001_80005' '80005' '200001' 1 1.0
 1.0 1 1 5 1 '200001_80005_2016-03-31 18:11:20' None None None None None
 None None None None None None '200001_80005_2016-03-31 18:11:20'
 0.019200000911951065 0.0 0.0 2 2 '1,,NULL' '4,0,NULL'
 '200001_80005_2016-03-31 18:11:20' ',NULL,NULL' ',NULL,NULL' ',NULL,NULL'
 1 1 1 ',NULL,NULL' ',NULL,NULL']
---------------predict change of purchase -------------
[[b'0.006222:0']]
```


## 附录A -- OneFlow定制代码编译
此章节介绍为OneEmbedding推理服务所定制的代码修改的编译过程。该代码会尽快合入OneFlow中，届时以下步骤可省略。

### A.1 容器环境准备
使用以下容器环境进行编译。该容器已经安装编译所需的依赖等。
```
docker pull registry.cn-beijing.aliyuncs.com/oneflow/triton-devel:latest
```
启动容器，请映射相关路径(映射本地目录至`/work/oneflow_demo`)，方便保存编译产物。接下来的操作均在容器内进行。

### A.2 编译OneFlow
```shell
cd /work/oneflow_demo
mkdir oneflow_serving && cd oneflow_serving
git clone -b support_oneembedding_serving --single-branch https://github.com/Oneflow-Inc/oneflow --depth=1 
cd oneflow
mkdir build && cd build
cmake -C ../cmake/caches/cn/cuda.cmake \
-DBUILD_CPP_API=ON \
-DWITH_MLIR=ON \
-G Ninja \
-DBUILD_SHARED_LIBS=ON \
-DBUILD_HWLOC=OFF \
-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=OFF \
-DCMAKE_EXE_LINKER_FLAGS_INIT="-fuse-ld=lld" \
-DCMAKE_MODULE_LINKER_FLAGS_INIT="-fuse-ld=lld" \
-DCMAKE_SHARED_LINKER_FLAGS_INIT="-fuse-ld=lld" ..
ninja
```

### A.3 编译Serving

```shell
git clone -b support_one_embedding --single-branchhttps://github.com/Oneflow-Inc/serving.git
cd serving
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=/path/to/liboneflow_cpp/share -DTRITON_RELATED_REPO_TAG=r21.10 \
  -DTRITON_ENABLE_GPU=ON -G Ninja -DTHIRD_PARTY_MIRROR=aliyun ..
ninja
```
上述命令中的`/path/to/liboneflow_cpp/share`要替换成上边编译的oneflow的里面的路径，在`{oneflow路径}/build/liboneflow_cpp/share`


### A.4 测试TritonServer
复制backend库文件：
```shell
mkdir /work/oneflow_demo/oneflow_sering/backends && cd /work/oneflow_demo/oneflow_sering/backends
mkdir oneflow
cp /roor/prject/oneflow_serving/serving/build/libtriton_oneflow.so oneflow/.
```

在命令行启动 TritonServer测试：
```shell
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/work/oneflow_demo/oneflow_serving/oneflow/build/liboneflow_cpp/lib \
/opt/tritonserver/bin/tritonserver \
--model-repository=/work/oneflow_demo/oneflow_process/model \
--backend-directory=/work/oneflow_demo/oneflow_serving/backends
```

在另一个命令行运行如下指令，若成功，输出示例如下：
```
python $demodir/serving/client.py 
>>>
0.045439958572387695
(1, 1)
[[b'0.025343:0']]
```
注意：
- 如果出现`libunwind.so.8`未找到需要用`-v /lib/x86_64-linux-gnu:/unwind_path` 映射一下`libunwind.so.8`所在目录，然后添加到`LD_LIBRARY_PATH`里面: `... bash -c 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mylib:/unwind_path ...`
- 如果出现`libcupti.so`未找到需要用`-v /usr/local/cuda-11.7/extras/CUPTI/lib64:/cupti_path` 映射一下`libcupti.so`所在目录，然后添加到`LD_LIBRARY_PATH`里面: `... bash -c 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mylib:/cupti_path ...`, 其中具体的cuda的路径按实际安装的位置，可以用`ldd {oneflow路径}/build/liboneflow.so | grep cupti`来找到

