# 操作之前要更新表
def refresh_all_table():
    # 更新表：
    spark.sql("refresh table launch_secure.launch_entries")
    spark.sql("refresh table airbot_prod.bot_master_all_risk_level_score")
    spark.sql("refresh table swatbots_secure.heuristics_predictions")
    spark.sql("refresh table airbot_prod.member_risk_score_exclude_demand")
    spark.sql("refresh table airbot_prod.kr_community")
    spark.sql("refresh table airbot_prod.jp_community")
    # airbot_prod.address_bot_pattern
    # airbot_prod.suspicious_forter_account
    # airbot_prod.susp_reg_detail
    # airbot_prod.profile_similarity_flag
    # airbot_prod.suspicious_forter_address
    # airbot_prod.forter_community_score
    # airbot_prod.community_score
    # airbot_prod.suspicious_email_account
    spark.sql("refresh table airbot_prod.address_bot_pattern")
    spark.sql("refresh table airbot_prod.suspicious_forter_account")
    spark.sql("refresh table airbot_prod.susp_reg_detail")
    spark.sql("refresh table airbot_prod.profile_similarity_flag")
    spark.sql("refresh table airbot_prod.suspicious_forter_address")
    spark.sql("refresh table airbot_prod.forter_community_score")
    spark.sql("refresh table airbot_prod.community_score")
    spark.sql("refresh table airbot_prod.suspicious_email_account")

# 没上模型的时候：
# 未上模型的函数
def get_non_pattern_launch_id_contrast_df(PRODUCT_COUNTRY, received_date):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # 读取table
    df = spark.table("launch_secure.launch_entries")

    filtered_df = df.filter((df["PRODUCT_MERCH_GROUP"] == PRODUCT_COUNTRY) & (df["received_date"] >= received_date))

    # 读取存储tier标签表
    high_low_heat_df = spark.table("launch.launch_info")

    # 定义窗口规范，按照launch_id分组，按照last_modified_date倒序排列
    window_spec = Window().partitionBy("launch_id").orderBy(F.col("last_modified_date").desc())

    # 使用row_number分配每个分组的行号
    ranked_df = high_low_heat_df.withColumn("row_number", F.row_number().over(window_spec))

    # 筛选出每个launch_id的最新记录
    high_low_heat_df = ranked_df.filter(F.col("row_number") == 1).drop("row_number")

    # 更换launch_id，received_date名，避免与entris表冲突
    high_low_heat_df = high_low_heat_df.withColumnRenamed("launch_id", "info_launch_id").withColumnRenamed(
        "received_date", "info_received_date")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        filtered_df.join(
            high_low_heat_df,
            F.col("LAUNCH_ID") == F.col("info_launch_id"),
            how="inner"
        )
    )

    # 对launch中的相同的upmid去重，遇到VALIDATION_RESULT有两种状态的upmid则保留两种状态各一个记录
    # 坑点：如果使用dropDuplicates(["UPMID", "VALIDATION_RESULT"])可能会将在其他launch中相同组合的"UPMID", "VALIDATION_RESULT",的upmid给删除，导致某些launch无法有匹配项
    joined_df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    # 窗口函数
    window_spec = Window().partitionBy("LAUNCH_ID")

    # 此处只进行一次join操作，统计量的计算正确
    result_df = (
        joined_df.
            # 假如不存在第一层反欺诈，本统计的是所有参加本次活动的人数
            withColumn("participants", F.count("*").over(window_spec))
            # 假如不存在第一层反欺诈，本统计的是（参加本次活动且能抢到货的全体总人数）
            .withColumn("winner", F.count(F.when(F.col("ENTRY_STATUS") == "WINNER", 1)).over(window_spec))
            # 1.增加invalid
            # 假如存在第一层反欺诈，本统计的是：（本层认为是bot的总bot数）--关注投放“机器人数量”
            .withColumn("invalid", F.count(F.when(F.col("VALIDATION_RESULT") == "INVALID", 1)).over(window_spec))
            # 假如存在第一层反欺诈，本统计的是：(本层认为是human的总human数)-- 关注“真人数”(非bot)
            .withColumn("valid", F.count(F.when(F.col("VALIDATION_RESULT") == "VALID", 1)).over(window_spec))
            .withColumn("ratio_winner_to_valid", F.round(F.col("winner") / F.col("valid"), 3))

    )

    filtered_result_df = result_df.filter(F.col("valid") >= 500)

    selected_result_df = (
        filtered_result_df.select(
            "LAUNCH_ID",
            "operational_tier",
            "UPMID",
            "PRODUCT_MERCH_GROUP",
            "PRODUCT_COUNTRY",
            "VALIDATION_RESULT",
            "ENTRY_STATUS",

            "participants",
            "invalid",
            "valid",

            "winner",

            # "ratio_winner_to_participants",
            "ratio_winner_to_valid",

            # "is_VALID",
            "received_date"
        )
    )

    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score")

    # 【坑点】打标签避免launch表中的upmid在risk_df中有多个匹配项，造成bot的统计出现多个重复结果，需要对链接的risk_df按照identity_upm去重复
    risk_df = risk_df.dropDuplicates(["identity_upm"])

    # Join DataFrames on identity_upm
    joined_df = (
        selected_result_df.join(
            risk_df,
            F.col("UPMID") == F.col("identity_upm"),
            how="left"
        )
    )

    # risk 表已经增加字段score_w_human_exclude_demand
    # # 通过demand的score>0过滤掉human_flag，只剩下suspicious
    # susp_df=spark.table("airbot_prod.member_risk_score_exclude_demand").withColumnRenamed("identity_upm","susp_upm").withColumnRenamed("score","susp_score").withColumnRenamed("desc","susp_desc").dropDuplicates(["susp_upm"])

    # # 过滤掉human，只留下suspicious
    # joined_df = (
    # joined_df.join(
    #     susp_df,
    #     (F.col("UPMID")==F.col("susp_upm")),
    #     how="left"
    #     )
    # )
    # joined_df = joined_df.dropDuplicates(["LAUNCH_ID","UPMID", "VALIDATION_RESULT"])

    # 阈值是35时
    result_with_bot_ind_h = joined_df.withColumn(
        "bot_35",
        F.when((F.col("score_w_human_exclude_demand") >= 35), 1).otherwise(0)
        # F.when((F.col("score") >= 35) , 1).otherwise(0)
    )

    # 阈值20
    result_with_bot_ind_h = result_with_bot_ind_h.withColumn(
        "bot_20",
        # F.when((F.col("score_w_human") >= 35) & (F.col("bot_master_human") != 1), 1).otherwise(0)
        F.when((F.col("score_w_human_exclude_demand") >= 20), 1).otherwise(0)
        # F.when((F.col("score") >= 35) , 1).otherwise(0)
    )

    # 无阈值时的
    result_with_bot_ind_h = result_with_bot_ind_h.withColumn(
        "suspicous",
        # F.when((F.col("score_w_human") >= 35) & (F.col("bot_master_human") != 1), 1).otherwise(0)
        # score_w_human_exclude_demand 是risk表中的数据
        F.when(F.col("score_w_human_exclude_demand") > 0, 1).otherwise(0)
        # F.when((F.col("score") >= 35) , 1).otherwise(0)
    )

    # 进行规则表的打标签：没上模型时
    # 模型进行打标签：swatbots_secure.themis_predictions
    # 先更换upmid为别名，因为这与活动表的UPMID重名，且dataframe似乎不区分table的大小写
    predic_df = spark.table("swatbots_secure.heuristics_predictions").filter(
        (F.col("launch_date") >= received_date) & (F.col("merch_group") == PRODUCT_COUNTRY)).withColumnRenamed("upmid",
                                                                                                               "predic_upmid")

    # 阻止别的launch中的upmid也跑来进行匹配。dropDuplicates(["upmid"])谨慎使用，此时业务需要保留相同的upmid，因为同个upmid可能在多个launch中
    # 取出规则表中各个launch时间下的对应的upmid。-- 限制upmid出现多个匹配项。在一个launch下

    joined_df = (
        result_with_bot_ind_h.join(
            predic_df,
            # result_with_bot_ind_h["identity_upm"] == predic_df["predic_upmid"],
            (F.col("UPMID") == F.col("predic_upmid")) & (F.col("launchid") == F.col("LAUNCH_ID")),
            how="left"
        )
    )

    # select * from swatbots_secure.ml_predictions_launch_bots
    ml_predic_df = spark.table("swatbots_secure.ml_predictions_launch_bots").dropDuplicates(
        ["launch_id", "upmid"]).withColumnRenamed("launch_id", "ml_launch_id").withColumnRenamed("upmid",
                                                                                                 "ml_upmid").withColumnRenamed(
        "received_date", "ml_received_data")

    joined_df = (
        joined_df.join(
            ml_predic_df,
            (F.col("UPMID") == F.col("ml_upmid")) & (F.col("ml_launch_id") == F.col("LAUNCH_ID")),
            how="left"
        )
    )

    # joined_df = joined_df.dropDuplicates(["UPMID", "VALIDATION_RESULT"])
    joined_df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    window_spec = Window().partitionBy("LAUNCH_ID", "received_date")

    # (1)risk_bot
    count_risk_bot_df = (
        joined_df.withColumn("valid_MRS_bot_35",
                             F.count(F.when((F.col("bot_35") == 1) & (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(
                                 window_spec))
            .withColumn("valid_MRS_bot_20",
                        F.count(F.when((F.col("bot_20") == 1) & (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(
                            window_spec))
            .withColumn("valid_MRS_bot",
                        F.count(F.when((F.col("suspicous") == 1) & (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(
                            window_spec))
            # 2.增加invalid
            .withColumn("invalid_MRS_bot",
                        F.count(F.when((F.col("suspicous") == 1) & (F.col("VALIDATION_RESULT") == "INVALID"), 1)).over(
                            window_spec))
            .withColumn("winner_MRS_bot_35", F.count(F.when(
            (F.col("bot_35") == 1) & (F.col("ENTRY_STATUS") == "WINNER") & (F.col("VALIDATION_RESULT") == "VALID"),
            1)).over(window_spec))
            .withColumn("winner_MRS_bot_20", F.count(F.when(
            (F.col("bot_20") == 1) & (F.col("ENTRY_STATUS") == "WINNER") & (F.col("VALIDATION_RESULT") == "VALID"),
            1)).over(window_spec))
            .withColumn("winner_MRS_bot", F.count(F.when(
            (F.col("suspicous") == 1) & (F.col("ENTRY_STATUS") == "WINNER") & (F.col("VALIDATION_RESULT") == "VALID"),
            1)).over(window_spec))
    )

    count_ml_bot_df = (
        count_risk_bot_df.withColumn("valid_ml_bot", F.count(
            F.when((F.col("is_bot") == 1) & (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec)).withColumn(
            "winner_ml_bot", F.count(F.when(
                (F.col("is_bot") == 1) & (F.col("ENTRY_STATUS") == "WINNER") & (F.col("VALIDATION_RESULT") == "VALID"),
                1)).over(window_spec))
            # 3.增加invalid
            .withColumn("invalid_ml_bot",
                        F.count(F.when((F.col("is_bot") == 1) & (F.col("VALIDATION_RESULT") == "INVALID"), 1)).over(
                            window_spec))
    )

    # (2)predic_bot
    count_predic_bot_df = (
        count_ml_bot_df
            .withColumn("valid_heuristics_bot",
                        F.sum(F.when((F.col("heuristics_is_human") == 0) & (F.col("VALIDATION_RESULT")
                                                                            == "VALID"), 1)).over(window_spec))
            # 4.增加invalid
            .withColumn("invalid_heuristics_bot",
                        F.sum(F.when((F.col("heuristics_is_human") == 0) & (F.col("VALIDATION_RESULT")
                                                                            == "INVALID"), 1)).over(window_spec))
            .withColumn("winner_heuristics_bot", F.count(F.when(
            (F.col("heuristics_is_human") == 0) & (F.col("ENTRY_STATUS") == "WINNER") & (
                        F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))

            # 计算fairness
            # 增加predict_fairness、actual_fairness、perc_drop_in_fairness_accuracy
            # 使用两列生产新列不能使用在窗口中。
            .withColumn("winner_drop_heuristics_bot", F.col("winner") - F.col("winner_heuristics_bot"))
            .withColumn("heuristics_fairness",
                        F.round((F.col("winner_drop_heuristics_bot") / F.col("winner")) * 100, 2))
            .withColumn("winner_drop_MRS_bot_35", F.col("winner") - F.col("winner_MRS_bot_35"))
            .withColumn("winner_drop_MRS_bot_20", F.col("winner") - F.col("winner_MRS_bot_20"))
            .withColumn("winner_drop_MRS_bot", F.col("winner") - F.col("winner_MRS_bot"))
            .withColumn("MRS_fairness_35", F.round((F.col("winner_drop_MRS_bot_35") / F.col("winner")) * 100, 2))
            .withColumn("MRS_fairness_20", F.round((F.col("winner_drop_MRS_bot_20") / F.col("winner")) * 100, 2))
            .withColumn("MRS_fairness", F.round((F.col("winner_drop_MRS_bot") / F.col("winner")) * 100, 2))
            .withColumn("winner_drop_ml_bot", (F.col("winner") - F.col("winner_ml_bot")))
            .withColumn("ml_fairness", F.round((F.col("winner_drop_ml_bot") / F.col("winner")) * 100, 2))
            .withColumn("heuristics_drop_MRS_35_fairness_accuracy",
                        F.round(F.col("heuristics_fairness") - F.col("MRS_fairness_35"), 2))
            .withColumn("heuristics_drop_MRS_20_fairness_accuracy",
                        F.round(F.col("heuristics_fairness") - F.col("MRS_fairness_20"), 2))
            .withColumn("heuristics_drop_MRS_fairness_accuracy",
                        F.round(F.col("heuristics_fairness") - F.col("MRS_fairness"), 2))

    )

    # 增加统计字段，统计risk判定为bot，规则表也为bot的数据量，如果可行
    #  (3)both_bot

    # （1）valid/invalid & bot_ind_h=1 & heuristics_is_human=0
    count_risk_predic_bot_df = (
        count_predic_bot_df.withColumn("valid_MRS_heuristics_bot_35", F.count(F.when(
            (F.col("bot_35") == 1) & (F.col("heuristics_is_human") == 0) & (F.col("VALIDATION_RESULT") == "VALID"),
            1)).over(window_spec))
            .withColumn("valid_MRS_heuristics_bot_20", F.count(F.when(
            (F.col("bot_20") == 1) & (F.col("heuristics_is_human") == 0) & (F.col("VALIDATION_RESULT") == "VALID"),
            1)).over(window_spec))
            .withColumn("valid_MRS_heuristics_bot", F.count(F.when(
            (F.col("suspicous") == 1) & (F.col("heuristics_is_human") == 0) & (F.col("VALIDATION_RESULT") == "VALID"),
            1)).over(window_spec))
            .withColumn("winner_MRS_heuristics_bot_20", F.count(F.when(
            (F.col("ENTRY_STATUS") == "WINNER") & (F.col("bot_20") == 1) & (F.col("heuristics_is_human") == 0) & (
                        F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            .withColumn("winner_MRS_heuristics_bot_35", F.count(F.when(
            (F.col("ENTRY_STATUS") == "WINNER") & (F.col("bot_35") == 1) & (F.col("heuristics_is_human") == 0) & (
                        F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            .withColumn("winner_MRS_heuristics_bot", F.count(F.when(
            (F.col("ENTRY_STATUS") == "WINNER") & (F.col("suspicous") == 1) & (F.col("heuristics_is_human") == 0) & (
                        F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
    )

    # 筛选需要的字段
    flag_reasult_df = (
        count_risk_predic_bot_df.select(

            "LAUNCH_ID",
            "operational_tier",
            # "UPMID",
            "PRODUCT_MERCH_GROUP",
            # "PRODUCT_COUNTRY",
            # "VALIDATION_RESULT",
            # "ENTRY_STATUS",

            "participants",
            "invalid",
            "valid",
            "winner",
            # "count_L1_bot",
            "invalid_ml_bot",
            "invalid_heuristics_bot",
            "invalid_MRS_bot",

            "valid_ml_bot",
            "valid_heuristics_bot",

            "valid_MRS_bot",
            "valid_MRS_bot_20",
            "valid_MRS_bot_35",
            "valid_MRS_heuristics_bot",
            "valid_MRS_heuristics_bot_20",
            "valid_MRS_heuristics_bot_35",

            # "count_L1_human_winner",
            "winner_ml_bot",
            "winner_heuristics_bot",

            "winner_MRS_bot",
            "winner_MRS_bot_20",
            "winner_MRS_bot_35",
            "winner_MRS_heuristics_bot",
            "winner_MRS_heuristics_bot_20",
            "winner_MRS_heuristics_bot_35",

            "ratio_winner_to_valid",

            "winner_drop_ml_bot",
            "winner_drop_heuristics_bot",
            "winner_drop_MRS_bot",
            "winner_drop_MRS_bot_20",
            "winner_drop_MRS_bot_35",

            "ml_fairness",
            "heuristics_fairness",
            "MRS_fairness",
            "MRS_fairness_20",
            "MRS_fairness_35",
            "heuristics_drop_MRS_fairness_accuracy",
            "heuristics_drop_MRS_20_fairness_accuracy",
            "heuristics_drop_MRS_35_fairness_accuracy",

            "received_date"
        )
    )

    # 使用窗口函数按照 received_date 排序，保留每个分组的第一条记录
    grouped_flag_reasult_df = flag_reasult_df.withColumn(
        "row_number",
        F.row_number().over(Window.partitionBy("LAUNCH_ID", "received_date").orderBy("received_date"))
    ).filter("row_number = 1").drop("row_number")

    return grouped_flag_reasult_df


# 上模型
def get_pattern_launch_id_contrast_df(PRODUCT_COUNTRY, received_date):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    # import pandas as pd
    # 读取table
    df = spark.table("launch_secure.launch_entries")

    # launch表的upmid自带valid/invalid标签--不是launch的标签，launch_id的标签在info表中的tier字段
    # launch_id的区域要使用PRODUCT_MERCH_GROUP(大区域)去指定，不能使用PRODUCT_COUNTRY(小区域)
    filtered_df = df.filter((df["PRODUCT_MERCH_GROUP"] == PRODUCT_COUNTRY) & (df["received_date"] >= received_date))

    # 关注launch.launch_info的operational_tier字段和launch_id。通过launch_id字段进行连接打标签
    high_low_heat_df = spark.table("launch.launch_info")

    # 定义窗口规范，按照launch_id分组，按照last_modified_date倒序排列
    window_spec = Window().partitionBy("launch_id").orderBy(F.col("last_modified_date").desc())

    # 使用row_number分配每个分组的行号
    ranked_df = high_low_heat_df.withColumn("row_number", F.row_number().over(window_spec))

    # 筛选出每个launch_id的最新记录
    high_low_heat_df = ranked_df.filter(F.col("row_number") == 1).drop("row_number")

    # 更换launch_id，received_date名，避免与entris表冲突
    high_low_heat_df = high_low_heat_df.withColumnRenamed("launch_id", "info_launch_id").withColumnRenamed(
        "received_date", "info_received_date")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        filtered_df.join(
            high_low_heat_df,
            F.col("LAUNCH_ID") == F.col("info_launch_id"),
            how="inner"
        )
    )

    # 对launch中的相同的upmid去重，遇到VALIDATION_RESULT有两种状态的upmid则保留两种状态各一个记录
    # 坑点：如果使用dropDuplicates(["UPMID", "VALIDATION_RESULT"])可能会将在其他launch中相同组合的"UPMID", "VALIDATION_RESULT",的upmid给删除，导致某些launch无法有匹配项
    joined_df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    # 窗口函数
    window_spec = Window().partitionBy("LAUNCH_ID")

    # 此处只进行一次join操作，统计量的计算正确
    result_df = (
        joined_df.
            # 假如不存在第一层反欺诈，本统计的是所有参加本次活动的人数
            withColumn("participants", F.count("*").over(window_spec))
            # 假如不存在第一层反欺诈，本统计的是（参加本次活动且能抢到货的全体总人数）
            .withColumn("winner", F.count(F.when(F.col("ENTRY_STATUS") == "WINNER", 1)).over(window_spec))
            # 假如不存在第一层反欺诈，本统计的是：(抢到货物的总人数/参加活动的总人数)
            .withColumn("ratio_winner_to_participants", F.round(F.col("winner") / F.col("participants"), 3))

            # 假如存在第一层反欺诈，本统计的是：(本层认为是human的总human数)-- 关注“真人数”(非bot)
            .withColumn("valid", F.count(F.when(F.col("VALIDATION_RESULT") == "VALID", 1)).over(window_spec))
            # 1.增加invalid
            # 假如存在第一层反欺诈，本统计的是：（本层认为是bot的总bot数）--关注投放“机器人数量”
            .withColumn("invalid", F.count(F.when(F.col("VALIDATION_RESULT") == "INVALID", 1)).over(window_spec))
            # 假如存在第一层反欺诈，本统计的本层认为是:（本层认为是human中抢到货物的总human数）-- 关注活动“真人抢到数”
            #    .withColumn("count_winner", F.count(F.when((F.col("ENTRY_STATUS") == "WINNER")&(F.col("VALIDATION_RESULT")=="VALID"), 1)).over(window_spec))
            # 假如存在第一层反欺诈，本统计的是: (本层认为是human中抢到货物的总human数)/(总human人数)--关注活动”真人抢到货“与”真人数量“比
            .withColumn("ratio_winner_to_valid", F.round(F.col("winner") / F.col("valid"), 3))

    )

    # 挑出winner占比比较高，VALID数量比较多的launch
    filtered_result_df = result_df.filter(F.col("valid") >= 500)

    selected_result_df = (
        filtered_result_df.select(
            "LAUNCH_ID",
            "operational_tier",
            "UPMID",
            "PRODUCT_MERCH_GROUP",
            "PRODUCT_COUNTRY",
            "VALIDATION_RESULT",
            "ENTRY_STATUS",

            "participants",
            "invalid",
            "valid",
            # "count_L1_bot",

            "winner",
            # "count_L1_human_winner",
            # "count_L1_bot_winner",

            "ratio_winner_to_participants",
            "ratio_winner_to_valid",

            "received_date"
        )
    )

    # 2）使用
    # 先使用risk进行打标签
    # 经检查，每次打标签都要进行统计量计算。
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score")

    # 【坑点】打标签避免launch表中的upmid在risk_df中有多个匹配项，造成bot的统计出现多个重复结果，需要对链接的risk_df按照identity_upm去重复
    risk_df = risk_df.dropDuplicates(["identity_upm"])

    # Join DataFrames on identity_upm
    joined_df = (
        selected_result_df.join(
            risk_df,
            F.col("UPMID") == F.col("identity_upm"),
            how="left"
        )
    )

    # 通过demand的score>0过滤掉human_flag，只剩下suspicious
    # susp_df=spark.table("airbot_prod.member_risk_score_exclude_demand").withColumnRenamed("identity_upm","susp_upm").withColumnRenamed("score","susp_score").withColumnRenamed("desc","susp_desc").dropDuplicates(["susp_upm"])
    # # 过滤掉human，只留下suspicious
    # joined_df = (
    # joined_df.join(
    #     susp_df,
    #     (F.col("UPMID")==F.col("susp_upm")),
    #     how="left"
    #     )
    # )
    # joined_df = joined_df.dropDuplicates(["LAUNCH_ID","UPMID", "VALIDATION_RESULT"])

    # 35阈值
    result_with_bot_ind_h = joined_df.withColumn(
        "bot_35",
        F.when((F.col("score_w_human_exclude_demand") >= 35), 1).otherwise(0)
        # F.when((F.col("score") >= 35) , 1).otherwise(0)
    )

    # 阈值20
    result_with_bot_ind_h = result_with_bot_ind_h.withColumn(
        "bot_20",
        F.when((F.col("score_w_human_exclude_demand") >= 20), 1).otherwise(0)
        # F.when((F.col("score") >= 35) , 1).otherwise(0)
    )

    # 无阈值时
    result_with_bot_ind_h = result_with_bot_ind_h.withColumn(
        "suspicous",
        F.when((F.col("score_w_human_exclude_demand") > 0), 1).otherwise(0)
        # F.when((F.col("score") >= 35) , 1).otherwise(0)
    )

    # 进行规则表的打标签：没上模型时
    # 模型进行打标签：swatbots_secure.themis_predictions
    # 此处有问题
    predic_df = spark.table("swatbots_secure.themis_predictions").filter(
        (F.col("day") >= received_date) & (F.col("merch_group") == PRODUCT_COUNTRY)).withColumnRenamed("upmid",
                                                                                                       "predic_upmid")

    joined_df = (
        result_with_bot_ind_h.join(
            predic_df,
            # result_with_bot_ind_h["identity_upm"] == predic_df["predic_upmid"],
            # F.col("UPMID") == F.col("predic_upmid"),
            # 注意时间匹配
            (F.col("UPMID") == F.col("predic_upmid")) & (F.col("received_date") == F.col("day")),
            how="left"
        )
    )

    ml_predic_df = spark.table("swatbots_secure.ml_predictions_launch_bots").dropDuplicates(
        ["launch_id", "upmid"]).withColumnRenamed("launch_id", "ml_launch_id").withColumnRenamed("upmid",
                                                                                                 "ml_upmid").withColumnRenamed(
        "received_date", "ml_received_data")

    joined_df = (
        joined_df.join(
            ml_predic_df,
            (F.col("UPMID") == F.col("ml_upmid")) & (F.col("ml_launch_id") == F.col("LAUNCH_ID")),
            how="left"
        )
    )

    # joined_df = joined_df.dropDuplicates(["UPMID", "VALIDATION_RESULT"])
    joined_df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    window_spec = Window().partitionBy("LAUNCH_ID", "received_date")

    # (1)risk_bot
    count_risk_bot_df = (
        joined_df.withColumn(
            "valid_MRS_bot_35",
            F.count(F.when((F.col("bot_35") == 1) & (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            .withColumn(
            "valid_MRS_bot_20",
            F.count(F.when((F.col("bot_20") == 1) & (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            .withColumn(
            "valid_MRS_bot",
            F.count(F.when((F.col("suspicous") == 1) & (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            # 2.增加invalid
            .withColumn(
            "invalid_MRS_bot",
            F.count(F.when((F.col("suspicous") == 1) & (F.col("VALIDATION_RESULT") == "INVALID"), 1)).over(window_spec))
            .withColumn(
            "winner_MRS_bot_35",
            F.count(F.when((F.col("bot_35") == 1) & (F.col("ENTRY_STATUS") == "WINNER") &
                           (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            .withColumn(
            "winner_MRS_bot_20",
            F.count(F.when((F.col("bot_20") == 1) & (F.col("ENTRY_STATUS") == "WINNER") &
                           (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            .withColumn(
            "winner_MRS_bot",
            F.count(F.when((F.col("suspicous") == 1) & (F.col("ENTRY_STATUS") == "WINNER") &
                           (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
    )

    count_ml_bot_df = (
        count_risk_bot_df.withColumn("valid_ml_bot", F.count(
            F.when((F.col("is_bot") == 1) & (F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            # 3.增加invalid
            .withColumn("invalid_ml_bot",
                        F.count(F.when((F.col("is_bot") == 1) & (F.col("VALIDATION_RESULT") == "INVALID"), 1)).over(
                            window_spec))
            .withColumn("winner_ml_bot", F.count(F.when(
            (F.col("is_bot") == 1) & (F.col("ENTRY_STATUS") == "WINNER") & (F.col("VALIDATION_RESULT") == "VALID"),
            1)).over(window_spec))
    )

    # (2)predic_bot
    count_predic_bot_df = (
        count_ml_bot_df
            .withColumn("valid_themis_bot", F.sum(F.when((F.col("is_human") == 0) & (F.col("VALIDATION_RESULT")
                                                                                     == "VALID"), 1)).over(window_spec))
            # 4.增加invalid
            .withColumn("invalid_themis_bot", F.sum(F.when((F.col("is_human") == 0) & (F.col("VALIDATION_RESULT")
                                                                                       == "INVALID"), 1)).over(
            window_spec))

            .withColumn("winner_themis_bot", F.count(F.when(
            (F.col("is_human") == 0) & (F.col("ENTRY_STATUS") == "WINNER") & (F.col("VALIDATION_RESULT") == "VALID"),
            1)).over(window_spec))

            # 计算fairness
            # 增加predict_fairness、actual_fairness、perc_drop_in_fairness_accuracy
            # 使用两列生产新列不能使用在窗口中。
            .withColumn("winner_drop_themis_bot", F.col("winner") - F.col("winner_themis_bot"))
            .withColumn("themis_fairness", F.round((F.col("winner_drop_themis_bot") / F.col("winner")) * 100, 2))
            .withColumn("winner_drop_MRS_bot_35", F.col("winner") - F.col("winner_MRS_bot_35"))
            .withColumn("winner_drop_MRS_bot_20", F.col("winner") - F.col("winner_MRS_bot_20"))
            .withColumn("winner_drop_MRS_bot", F.col("winner") - F.col("winner_MRS_bot"))
            .withColumn("MRS_fairness_35", F.round((F.col("winner_drop_MRS_bot_35") / F.col("winner")) * 100, 2))
            .withColumn("MRS_fairness_20", F.round((F.col("winner_drop_MRS_bot_20") / F.col("winner")) * 100, 2))
            .withColumn("MRS_fairness", F.round((F.col("winner_drop_MRS_bot") / F.col("winner")) * 100, 2))
            .withColumn("winner_drop_ml_bot", (F.col("winner") - F.col("winner_ml_bot")))
            .withColumn("ml_fairness", F.round((F.col("winner_drop_ml_bot") / F.col("winner")) * 100, 2))
            .withColumn("themis_drop_MRS_35_fairness_accuracy",
                        F.round(F.col("themis_fairness") - F.col("MRS_fairness_35"), 2))
            .withColumn("themis_drop_MRS_20_fairness_accuracy",
                        F.round(F.col("themis_fairness") - F.col("MRS_fairness_20"), 2))
            .withColumn("themis_drop_MRS_fairness_accuracy",
                        F.round(F.col("themis_fairness") - F.col("MRS_fairness"), 2))

    )

    # （1）valid/invalid & bot_ind_h=1 & heuristics_is_human=0
    count_risk_predic_bot_df = (
        count_predic_bot_df.withColumn("valid_MRS_themis_bot_35", F.count(
            F.when((F.col("bot_35") == 1) & (F.col("is_human") == 0) & (F.col("VALIDATION_RESULT") == "VALID"),
                   1)).over(window_spec))
            .withColumn("valid_MRS_themis_bot_20", F.count(
            F.when((F.col("bot_20") == 1) & (F.col("is_human") == 0) & (F.col("VALIDATION_RESULT") == "VALID"),
                   1)).over(window_spec))
            .withColumn("valid_MRS_themis_bot", F.count(
            F.when((F.col("suspicous") == 1) & (F.col("is_human") == 0) & (F.col("VALIDATION_RESULT") == "VALID"),
                   1)).over(window_spec))
            .withColumn("winner_MRS_themis_bot_35", F.count(F.when(
            (F.col("ENTRY_STATUS") == "WINNER") & (F.col("bot_35") == 1) & (F.col("is_human") == 0) & (
                        F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            .withColumn("winner_MRS_themis_bot_20", F.count(F.when(
            (F.col("ENTRY_STATUS") == "WINNER") & (F.col("bot_20") == 1) & (F.col("is_human") == 0) & (
                        F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
            .withColumn("winner_MRS_themis_bot", F.count(F.when(
            (F.col("ENTRY_STATUS") == "WINNER") & (F.col("suspicous") == 1) & (F.col("is_human") == 0) & (
                        F.col("VALIDATION_RESULT") == "VALID"), 1)).over(window_spec))
    )

    # 筛选需要的字段
    flag_reasult_df = (
        count_risk_predic_bot_df.select(

            "LAUNCH_ID",
            "operational_tier",
            # "UPMID",
            "PRODUCT_MERCH_GROUP",
            # "PRODUCT_COUNTRY",
            # "VALIDATION_RESULT",
            # "ENTRY_STATUS",

            "participants",
            "invalid",
            "valid",
            # "count_L1_bot",
            "winner",

            "invalid_ml_bot",
            "invalid_themis_bot",
            "invalid_MRS_bot",

            "valid_ml_bot",
            "valid_themis_bot",

            "valid_MRS_bot",
            "valid_MRS_bot_20",
            "valid_MRS_bot_35",
            "valid_MRS_themis_bot",
            "valid_MRS_themis_bot_20",
            "valid_MRS_themis_bot_35",

            "winner_ml_bot",
            "winner_themis_bot",

            "winner_MRS_bot",
            "winner_MRS_bot_20",
            "winner_MRS_bot_35",
            "winner_MRS_themis_bot",
            "winner_MRS_themis_bot_20",
            "winner_MRS_themis_bot_35",

            # "ratio_winner_to_participants",
            "ratio_winner_to_valid",

            "winner_drop_ml_bot",
            "winner_drop_themis_bot",
            "winner_drop_MRS_bot",
            "winner_drop_MRS_bot_20",
            "winner_drop_MRS_bot_35",

            "ml_fairness",
            "themis_fairness",
            "MRS_fairness",
            "MRS_fairness_20",
            "MRS_fairness_35",

            "themis_drop_MRS_fairness_accuracy",
            "themis_drop_MRS_20_fairness_accuracy",
            "themis_drop_MRS_35_fairness_accuracy",

            "received_date"
        )
    )

    # 使用窗口函数按照 received_date 排序，保留每个分组的第一条记录
    grouped_flag_reasult_df = flag_reasult_df.withColumn(
        "row_number",
        F.row_number().over(Window.partitionBy("LAUNCH_ID", "received_date").orderBy("received_date"))
    ).filter("row_number = 1").drop("row_number")

    return grouped_flag_reasult_df

# grouped_flag_reasult_df.createOrReplaceTempView("tmp_of_launch_entries_KR_distinct_launch_id")



# 使用案例
spark.sql("SET spark.databricks.queryWatchdog.maxQueryTasks=150000")
get_non_pattern_launch_id_contrast_df("KR","2023-07-01").display()