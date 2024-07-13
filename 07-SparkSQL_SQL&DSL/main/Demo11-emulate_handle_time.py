"""
以下代码执行时间需要3分钟

"""

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# 数据量：124665
human_df = spark.read.csv('s3://ngap--customer-data-science--prod--us-east-1/airbot/uat/all_swoosh_upmid_app_os_7d.csv',
                          header=True).select("upmid")
# 数据量：4386150
forter_community_df = spark.table("airbot_prod.forter_community_score").select("upmid", "address", "community_id")

# 检查DataFrame是否已经被缓存
if not forter_community_df.is_cached:
    # 如果未被缓存，则进行缓存
    forter_community_df.persist(StorageLevel.MEMORY_AND_DISK)

# forter_community_df.display()

# # 数据量：825328026--8亿2千多万
mrs_df = spark.table("airbot_prod.bot_master_all_risk_level_score_dev").select("identity_upm", "score", "score_w_human",
                                                                               "reason").filter(
    F.array_contains("reason", "forter_community"))

# 先将human_df进行mrs打forter标签筛选出foter数据源，
joined_mrs_df = human_df.join(mrs_df, mrs_df.identity_upm == human_df.upmid, "inner").drop(mrs_df.identity_upm)
# print(f"join mrs_forter后的数量：{joined_mrs_df.count()}")
# 再通过forter_community_df打community_id标签，获取community_id,address
joined_forter_community_df = joined_mrs_df.join(forter_community_df, joined_mrs_df.upmid == forter_community_df.upmid,
                                                "inner").drop(forter_community_df.upmid).dropDuplicates(
    ["upmid", "address"])

if not joined_forter_community_df.is_cached:
    # 如果未被缓存，则进行缓存
    joined_forter_community_df.persist(StorageLevel.MEMORY_AND_DISK)

print(f"join forter_community后的数量：{joined_forter_community_df.count()}")

# 获取joined_forter_community_df的community_id
community_id_df = joined_forter_community_df.select("community_id").dropDuplicates(["community_id"])

if not community_id_df.is_cached:
    community_id_df.persist(StorageLevel.MEMORY_AND_DISK)

print(f"数据源的community_id数量：{community_id_df.count()}")

# 释放joined_forter_community_df
joined_forter_community_df.unpersist()

# community_id_df.display() # 前面都正确了

# 将所有中到的community_id全部找到出所有的地址
# 数据量：3570300
all_community_upmid_address_df = community_id_df.join(forter_community_df, ["community_id"], "inner").dropDuplicates(
    ["upmid", "community_id", "address"])

if not all_community_upmid_address_df.is_cached:
    all_community_upmid_address_df.persist(StorageLevel.MEMORY_AND_DISK)

community_id_df.unpersist()
print(f"所有community_id出来的数据量{all_community_upmid_address_df.count()}")

# all_community_upmid_address_df.display()

# L1层处理
source_df = all_community_upmid_address_df

# 数据量：3066710
L1_df = get_single_re_normal_address_upmid_df(source_df)

if not L1_df.is_cached:
    L1_df.persist(StorageLevel.MEMORY_AND_DISK)

all_community_upmid_address_df.unpersist()

print(f"第一层抽出的可能正常的地址数量：{L1_df.count()}")
