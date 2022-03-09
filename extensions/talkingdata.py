import gc
import os
from matplotlib.pyplot import table
import pandas as pd
import time
import numpy as np
from sklearn.model_selection import train_test_split
import lightgbm as lgb  # mac needs `brew install libomp`
# import sqlalchemy as db  # openmldb sqlalchemy doesn't support set/use sql
import openmldb.sdk


def lgb_modelfit_nocv(params, dtrain, dvalid, predictors, target='target', objective='binary', metrics='auc',
                      feval=None, early_stopping_rounds=20, num_boost_round=3000, verbose_eval=10,
                      categorical_features=None):
    lgb_params = {
        'boosting_type': 'gbdt',
        'objective': objective,
        'metric': metrics,
        'learning_rate': 0.01,
        # 'is_unbalance': 'true',  #because training data is unbalance (replaced with scale_pos_weight)
        'num_leaves': 31,  # we should let it be smaller than 2^(max_depth)
        'max_depth': -1,  # -1 means no limit
        # Minimum number of data need in a child(min_data_in_leaf)
        'min_child_samples': 20,
        'max_bin': 255,  # Number of bucketed bin for feature values
        'subsample': 0.6,  # Subsample ratio of the training instance.
        'subsample_freq': 0,  # frequence of subsample, <=0 means no enable
        # Subsample ratio of columns when constructing each tree.
        'colsample_bytree': 0.3,
        # Minimum sum of instance weight(hessian) needed in a child(leaf)
        'min_child_weight': 5,
        'subsample_for_bin': 200000,  # Number of samples for constructing bin
        'min_split_gain': 0,  # lambda_l1, lambda_l2 and min_gain_to_split to regularization
        'reg_alpha': 0,  # L1 regularization term on weights
        'reg_lambda': 0,  # L2 regularization term on weights
        'nthread': 8,
        'verbose': 0,
        'metric': metrics
    }

    lgb_params.update(params)

    print("preparing validation datasets")

    xgtrain = lgb.Dataset(dtrain[predictors].values, label=dtrain[target].values,
                          feature_name=predictors,
                          categorical_feature=categorical_features
                          )
    xgvalid = lgb.Dataset(dvalid[predictors].values, label=dvalid[target].values,
                          feature_name=predictors,
                          categorical_feature=categorical_features
                          )

    evals_results = {}

    bst1 = lgb.train(lgb_params,
                     xgtrain,
                     valid_sets=[xgtrain, xgvalid],
                     valid_names=['train', 'valid'],
                     evals_result=evals_results,
                     num_boost_round=num_boost_round,
                     early_stopping_rounds=early_stopping_rounds,
                     verbose_eval=10,
                     feval=feval)

    n_estimators = bst1.best_iteration
    print("\nModel Report")
    print("n_estimators : ", n_estimators)
    print(metrics + ":", evals_results['valid'][metrics][n_estimators - 1])

    return bst1


path = 'data/'

# use pandas extension types to support NA in integer column
dtypes = {
    'ip': 'UInt32',
    'app': 'UInt16',
    'device': 'UInt16',
    'os': 'UInt16',
    'channel': 'UInt16',
    'is_attributed': 'UInt8',
    'click_id': 'UInt32'
}

common_schema = [('ip', 'int'), ('app', 'int'), ('device', 'int'),
                 ('os', 'int'), ('channel', 'int'), ('click_time', 'timestamp')]
train_schema = common_schema + [('is_attributed', 'int')]
test_schema = common_schema + [('click_id', 'int')]
print('prepare train&test data...')

train_df = pd.read_csv(path + "train.csv", nrows=4000000,
                       dtype=dtypes, usecols=[c[0] for c in train_schema])

test_df = pd.read_csv(path + "test.csv", dtype=dtypes,
                      usecols=[c[0] for c in test_schema])

len_train = len(train_df)
# after appending, schema changed
# after append, two cols become float, can't set to int cuz NA
train_df = train_df.append(test_df)

# `hour`, `day` will be the groupby key
train_df['hour'] = pd.to_datetime(train_df.click_time).dt.hour.astype('uint8')
train_df['day'] = pd.to_datetime(train_df.click_time).dt.day.astype('uint8')

train_schema = train_schema + [('click_id', 'int'), ('hour', 'int'), ('day', 'int')]


def column_string(col_tuple) -> str:
    return ' '.join(col_tuple)


schema_string = ','.join(list(map(column_string, train_schema)))
print(train_df)
train_df.to_csv("train_prepared.csv", index=False)
# test_df.to_csv("test_prepared.csv", index=False)
del train_df
del test_df
gc.collect()

# macos python sdk got ImportError: dlopen(
# /usr/local/lib/python3.9/site-packages/openmldb/dbapi/../native/_sql_router_sdk.so, 2): initializer function
# 0x11611774c not in mapped image for /usr/local/lib/python3.9/site-packages/openmldb/dbapi/../native/_sql_router_sdk
# .so https://zhuanlan.zhihu.com/p/107024982 健康引用关系
# engine = db.create_engine(
#     'openmldb:///foo?zk=127.0.0.1:6181&zkPath=/openmldb')
# connection = engine.connect()
option = openmldb.sdk.OpenMLDBClusterSdkOptions("127.0.0.1:6181", "/openmldb")
sdk = openmldb.sdk.OpenMLDBSdk(option, True)
sdk.init()

db_name = "kaggle"
table_name = "talkingdata" + str(int(time.time()))
print("use openmldb db {} table {}".format(db_name, table_name))


# unsupported now
# connection.execute("set @@execute_mode='offline';")

def nothrow_execute(sql):
    try:
        print("execute " + sql)
        ok, rs = sdk.executeQuery(db_name, sql)
        print(rs)
    except Exception as e:
        print(e)


nothrow_execute("CREATE DATABASE {};".format(db_name))
nothrow_execute("USE {};".format(db_name))
nothrow_execute("CREATE TABLE {}({});".format(table_name, schema_string))

nothrow_execute("set @@execute_mode='offline';")
print("load data to offline storage for training")
nothrow_execute("LOAD DATA INFILE 'file://{}' INTO TABLE {}.{};".format(os.path.abspath("train_prepared.csv"), db_name, table_name))
# todo: must wait for load data finished

print('data prep...')
final_train_file = "final_train.csv"
sql_part = """
select ip, day, hour, 
count(channel) over w1 as qty, 
count(channel) over w2 as ip_app_count, 
count(channel) over w3 as ip_app_os_count
from {}.{} 
window 
w1 as (partition by ip, day, hour order by click_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
w2 as(partition by ip, day order by click_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
w3 as(partition by ip, app order by click_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
""".format(db_name, table_name)
nothrow_execute("{} INTO OUTFILE '{}';".format(sql_part, os.path.abspath(final_train_file)))

os._exit(233)

# # of clicks for each ip-day-hour combination
print('group by...')
gp = train_df[['ip', 'day', 'hour', 'channel']].groupby(by=['ip', 'day', 'hour'])[
    ['channel']].count().reset_index().rename(index=str, columns={'channel': 'qty'})
print('merge...')
train_df = train_df.merge(gp, on=['ip', 'day', 'hour'], how='left')
del gp
gc.collect()

# # of clicks for each ip-app combination
print('group by...')
gp = train_df[['ip', 'app', 'channel']].groupby(by=['ip', 'app'])[['channel']].count(
).reset_index().rename(index=str, columns={'channel': 'ip_app_count'})
train_df = train_df.merge(gp, on=['ip', 'app'], how='left')
del gp
gc.collect()

# # of clicks for each ip-app-os combination
print('group by...')
gp = train_df[['ip', 'app', 'os', 'channel']].groupby(by=['ip', 'app', 'os'])[['channel']].count(
).reset_index().rename(index=str, columns={'channel': 'ip_app_os_count'})
train_df = train_df.merge(gp, on=['ip', 'app', 'os'], how='left')
del gp
gc.collect()

print("vars and data type: ")
train_df.info()
train_df['qty'] = train_df['qty'].astype('uint16')
train_df['ip_app_count'] = train_df['ip_app_count'].astype('uint16')
train_df['ip_app_os_count'] = train_df['ip_app_os_count'].astype('uint16')

train_df.head(20)
test_df = train_df[len_train:]
val_df = train_df[(len_train - 3000000):len_train]
train_df = train_df[:(len_train - 3000000)]

print("train size: ", len(train_df))
print("valid size: ", len(val_df))
print("test size : ", len(test_df))

target = 'is_attributed'
predictors = ['app', 'device', 'os', 'channel', 'hour',
              'day', 'qty', 'ip_app_count', 'ip_app_os_count']
categorical = ['app', 'device', 'os', 'channel', 'hour']

sub = pd.DataFrame()
sub['click_id'] = test_df['click_id'].astype('int')

gc.collect()

print("Training...")
params = {
    'learning_rate': 0.1,
    # 'is_unbalance': 'true', # replaced with scale_pos_weight argument
    'num_leaves': 7,  # we should let it be smaller than 2^(max_depth)
    'max_depth': 3,  # -1 means no limit
    # Minimum number of data need in a child(min_data_in_leaf)
    'min_child_samples': 100,
    'max_bin': 100,  # Number of bucketed bin for feature values
    'subsample': 0.7,  # Subsample ratio of the training instance.
    'subsample_freq': 1,  # frequence of subsample, <=0 means no enable
    # Subsample ratio of columns when constructing each tree.
    'colsample_bytree': 0.7,
    # Minimum sum of instance weight(hessian) needed in a child(leaf)
    'min_child_weight': 0,
    'scale_pos_weight': 99  # because training data is extremely unbalanced
}
bst = lgb_modelfit_nocv(params,
                        train_df,
                        val_df,
                        predictors,
                        target,
                        objective='binary',
                        metrics='auc',
                        early_stopping_rounds=50,
                        verbose_eval=True,
                        num_boost_round=300,
                        categorical_features=categorical)

del train_df
del val_df
gc.collect()

print("Predicting...")
sub['is_attributed'] = bst.predict(test_df[predictors])
print("writing...")
sub.to_csv('sub_lgb_balanced99.csv', index=False)
print("done...")
print(sub.info())
