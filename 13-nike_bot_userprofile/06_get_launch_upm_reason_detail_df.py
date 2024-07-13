# 更新表的操作
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


def get_suspicious_community_kr_df(launch_id, winner=None):
    # 筛选包含suspicious_community_kr的suspicious_community_kr
    from pyspark.sql import functions as F

    # 抽出launch_id的前三个字符
    tag = launch_id[:3]

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数winner
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    # if threshold is not None:
    #   risk_df = risk_df.filter(
    #     (F.col("score")>=threshold)
    #   )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot,airbot_prod.kr_community
    # pattern_df = spark.table("airbot_prod.address_bot_pattern")
    # print("flag详情表可查询：airbot_prod.address_bot_pattern")
    pattern_df = spark.table("airbot_prod.kr_community").filter(F.col("community_id").isNotNull())
    print("flag详情表可查询：airbot_prod.address_bot_pattern & airbot_prod.kr_community")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()

    if winner is not None:
        print(f"launch_{tag}中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch_{tag}中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    # print("Filter out suspicious through the member_risk_score_exclude_demand table")
    # susp_upmid=df.count()
    # if winner is not None:
    #     print(f"launch_{tag}中winner_upmid中exclude_human_suspicious数量:{susp_upmid} (non_threshold)")
    # else:
    #     # valid_upmid=launch_df.count()
    #     print(f"launch_{tag}中valid_upmid中exclude_human_suspicious数量:{susp_upmid} (non_threshold)")

    # 打reason_flag

    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")

    # risk_upmid=df.count()
    # if winner is not None:
    #     print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    # else:
    #     # valid_upmid=launch_df.count()
    #     print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # non_threshold
    risk_upmid = df.count()
    # threshold_35
    risk_upmid_threshold_35 = df.filter(F.col("score") >= 35).count()

    if winner is not None:
        print(f"launch_{tag}中winner_upmid中risk_reason标签的数量:{risk_upmid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中risk_reason标签的数量:{risk_upmid_threshold_35} (threshold_35)")

    else:
        # valid_upmid=launch_df.count()
        print(f"launch_{tag}中valid_upmid中risk_reason标签的数量:{risk_upmid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中risk_reason标签的数量:{risk_upmid_threshold_35} (threshold_35)")

    # 避免冲突
    df = df.withColumnRenamed("SHIPPING_ADDRESS_COUNTRY", "launch_SHIPPING_ADDRESS_COUNTRY").withColumnRenamed(
        "SHIPPING_ADDRESS_CITY", "launch_SHIPPING_ADDRESS_CITY").withColumnRenamed("SHIPPING_ADDRESS_ADDRESS1",
                                                                                   "launch_SHIPPING_ADDRESS_ADDRESS1").withColumnRenamed(
        "SHIPPING_ADDRESS_ADDRESS2", "launch_SHIPPING_ADDRESS_ADDRESS2")

    # 更换launch_id，received_date名，避免与launch表冲突
    pattern_df = pattern_df.withColumnRenamed("upmid", "pattern_upmid")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            pattern_df,
            (F.col("UPMID") == F.col("pattern_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    # print(f"给launch表的upimd中打对应的risk_bot打kr_coummunity_id标签的数量:{df.count()}")

    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        "launch_SHIPPING_ADDRESS_COUNTRY",
        "launch_SHIPPING_ADDRESS_CITY",
        "launch_SHIPPING_ADDRESS_ADDRESS1",
        "launch_SHIPPING_ADDRESS_ADDRESS2",

        # "SHIPPING_ADDRESS_COUNTRY",
        # "SHIPPING_ADDRESS_CITY",
        # "SHIPPING_ADDRESS_ADDRESS1",
        # "SHIPPING_ADDRESS_ADDRESS2",
        "address",
        "community_id",
        "score",
        # "pattern_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # (1)suspicious_community_kr 4198

    suspicious_community_kr_upmid_df = df.filter(F.array_contains("reason", "suspicious_community_kr"))
    # non_threshold
    flag_upmid = suspicious_community_kr_upmid_df.count()
    # threshold_35
    flag_upmid_threshold_35 = suspicious_community_kr_upmid_df.filter(F.col("score") >= 35).count()
    if winner is not None:
        print(f"launch_{tag}中winner_upmid中suspicious_community_kr标签的数量:{flag_upmid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中suspicious_community_kr标签的数量:{flag_upmid_threshold_35} (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中suspicious_community_kr标签的数量:{flag_upmid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中suspicious_community_kr标签的数量:{flag_upmid_threshold_35} (threshold_35)")

    # non_threshold
    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    # threshold_35
    rato_flag_in_valid_35 = round((flag_upmid_threshold_35 / valid_upmid), 4)

    if winner is not None:
        print(f"launch_{tag}中winner_upmid中suspicious_community_kr标签与winner的比例:{rato_flag_in_valid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中suspicious_community_kr标签与winner的比例:{rato_flag_in_valid_35} (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中suspicious_community_kr标签与valid的比例:{rato_flag_in_valid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中suspicious_community_kr标签与valid的比例:{rato_flag_in_valid_35} (threshold_35)")

    print("END!!! return suspicious_community_kr_upmid_df (non_threshold)")
    return suspicious_community_kr_upmid_df


def get_address_propagation_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )
    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    pattern_df = spark.table("airbot_prod.address_bot_pattern")
    print("flag详情表可查询：airbot_prod.address_bot_pattern")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # 避免冲突
    df = df.withColumnRenamed("SHIPPING_ADDRESS_COUNTRY", "launch_SHIPPING_ADDRESS_COUNTRY").withColumnRenamed(
        "SHIPPING_ADDRESS_CITY", "launch_SHIPPING_ADDRESS_CITY").withColumnRenamed("SHIPPING_ADDRESS_ADDRESS1",
                                                                                   "launch_SHIPPING_ADDRESS_ADDRESS1").withColumnRenamed(
        "SHIPPING_ADDRESS_ADDRESS2", "launch_SHIPPING_ADDRESS_ADDRESS2")

    # 更换launch_id，received_date名，避免与launch表冲突
    pattern_df = pattern_df.withColumnRenamed("LAUNCH_ID", "pattern_launch_id").withColumnRenamed("received_date",
                                                                                                  "pattern_received_date").withColumnRenamed(
        "UPMID", "pattern_upmid").withColumnRenamed("reason", "pattern_reason")
    pattern_df = pattern_df.filter(F.array_contains(F.col("reason"), "address_propagation"))

    joined_df = (
        df.join(
            pattern_df,
            (F.col("LAUNCH_ID") == F.col("pattern_launch_id")) & (F.col("UPMID") == F.col("pattern_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的risk_bot打address_propagation标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        # "ENTRY_STATUS",
        "launch_SHIPPING_ADDRESS_COUNTRY",
        "launch_SHIPPING_ADDRESS_CITY",
        "launch_SHIPPING_ADDRESS_ADDRESS1",
        "launch_SHIPPING_ADDRESS_ADDRESS2",

        # "SHIPPING_ADDRESS_COUNTRY",
        # "SHIPPING_ADDRESS_CITY",
        # "SHIPPING_ADDRESS_ADDRESS1",
        # "SHIPPING_ADDRESS_ADDRESS2",
        #   "community_id",
        #   "kr_community_id",
        "score",
        "pattern_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # address_propagation_upmid_df = df.filter(F.array_contains("reason", "address_propagation"))
    # # address_propagation_upmid_df.createOrReplaceTempView("temp_of_launch_6d6_L1_human_in_risk_bot_address_propagation_upmid")
    # return address_propagation_upmid_df

    address_propagation_upmid_df = df.filter(F.array_contains("reason", "address_propagation"))
    flag_upmid = address_propagation_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中address_propagation标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中address_propagation标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中address_propagation标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中address_propagation标签与valid的比例:{rato_flag_in_valid}")
    return address_propagation_upmid_df


# 4.get_billing_info_with_multiple_account_df
def get_billing_info_with_multiple_account_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )
    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    info_df = spark.table("airbot_prod.suspicious_forter_account")
    print("flag详情表(可疑账户库)可查询：airbot_prod.suspicious_forter_account")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # 更换launch_id，received_date名，避免与launch表冲突
    # s筛选billing_user_count>=5的account_id
    info_df = info_df.filter(F.col("billing_user_count") >= 5)
    # 解决冲突
    info_df = info_df.withColumnRenamed("reason", "info_reason")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            info_df,
            (F.col("UPMID") == F.col("account_id")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中billing_info_with_multiple_account打标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        # "SHIPPING_ADDRESS_COUNTRY",
        # "SHIPPING_ADDRESS_CITY",
        # "SHIPPING_ADDRESS_ADDRESS1",
        # "SHIPPING_ADDRESS_ADDRESS2",
        # "kr_community_id",
        "billing_phone",
        "last4",
        "billing_user_count",
        "score",
        "info_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # (1)billing_info_with_multiple_account--1127
    # billing_info_with_multiple_account_upmid_df = df.filter(F.array_contains("reason", "billing_info_with_multiple_account"))
    # return billing_info_with_multiple_account_upmid_df

    billing_info_with_multiple_account_upmid_df = df.filter(
        F.array_contains("reason", "billing_info_with_multiple_account"))
    flag_upmid = billing_info_with_multiple_account_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中billing_info_with_multiple_account标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中billing_info_with_multiple_account标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中billing_info_with_multiple_account标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中billing_info_with_multiple_account标签与valid的比例:{rato_flag_in_valid}")
    return billing_info_with_multiple_account_upmid_df


# 5.get_user_profile_flag_df
def get_user_profile_flag_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )
    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    reg_df = spark.table("airbot_prod.susp_reg_detail")
    print("flag详情表可查询：airbot_prod.susp_reg_detail")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    reg_df = reg_df.withColumnRenamed("upmid", "reg_upmid").filter(F.col("register_bot_flag") == 1)

    # 更换launch_id，received_date名，避免与launch表冲突
    # 筛选billing_user_count>=5的account_id
    # reg_df = reg_df
    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            reg_df,
            (F.col("UPMID") == F.col("reg_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的user_profile_flag打标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        "full_name_dob",
        "primary_email",
        "reg_date_former",
        "registration_dttm",
        "reg_day_diff",
        "reg_min_diff",
        "behavior_offenses",
        # "register_bot_flag",
        "score",
        "reason",
        "up_to_date",
        "exec_date",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # (1)billing_info_with_multiple_account--1127
    # user_profile_flag_upmid_df = df.filter(F.array_contains("reason", "user_profile_flag"))
    # return user_profile_flag_upmid_df

    user_profile_flag_upmid_df = df.filter(F.array_contains("reason", "user_profile_flag"))
    flag_upmid = user_profile_flag_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中user_profile_flag标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中user_profile_flag标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中user_profile_flag标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中user_profile_flag标签与valid的比例:{rato_flag_in_valid}")
    return user_profile_flag_upmid_df


# 6.get_profile_similarity_flag_df
def get_profile_similarity_flag_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    profile_similarity_df = spark.table("airbot_prod.profile_similarity_flag").filter(
        F.col("profile_similarity_flag") == 1)
    print("flag详情表可查询：airbot_prod.profile_similarity_flag")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # 避免冲突
    df = df.withColumnRenamed("SHIPPING_ADDRESS_COUNTRY", "launch_SHIPPING_ADDRESS_COUNTRY").withColumnRenamed(
        "SHIPPING_ADDRESS_CITY", "launch_SHIPPING_ADDRESS_CITY").withColumnRenamed("SHIPPING_ADDRESS_ADDRESS1",
                                                                                   "launch_SHIPPING_ADDRESS_ADDRESS1").withColumnRenamed(
        "SHIPPING_ADDRESS_ADDRESS2", "launch_SHIPPING_ADDRESS_ADDRESS2").withColumnRenamed("profile_similarity_flag",
                                                                                           "launch_profile_similarity_flag")

    # profile_similarity_df = spark.table("airbot_prod.profile_similarity_flag")
    # profile_similarity_df = profile_similarity_df.filter(F.col("profile_similarity_flag") == 1)

    profile_similarity_df = profile_similarity_df.withColumnRenamed("upmid", "profile_upmid")

    joined_df = (
        df.join(
            profile_similarity_df,
            (F.col("UPMID") == F.col("profile_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的risk_bot打profile_similarity_flag标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        "primary_email",
        "full_name",
        "name_latin_given",
        "name_latin_family",
        "screen_name",

        "first_name_email_distance",
        "second_name_email_distance",
        "name_email_distance",
        "min_dist",
        "profile_similarity_flag",
        "score",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # profile_similarity_flag_upmid_df = df.filter(F.array_contains("reason", "profile_similarity_flag"))
    # return profile_similarity_flag_upmid_df

    profile_similarity_flag_upmid_df = df.filter(F.array_contains("reason", "profile_similarity_flag"))
    flag_upmid = profile_similarity_flag_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中profile_similarity_flag标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中profile_similarity_flag标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中profile_similarity_flag标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中profile_similarity_flag标签与valid的比例:{rato_flag_in_valid}")
    return profile_similarity_flag_upmid_df


# 7.get_billing_address_with_multiple_shipping_address_df
def get_billing_address_with_multiple_shipping_address_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )
    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    info_df = spark.table("airbot_prod.suspicious_forter_address").filter(F.col("billing_multi_shipping_count") >= 10)
    print("flag详情表可查询：airbot_prod.suspicious_forter_address")
    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # info_df = spark.table("airbot_prod.suspicious_forter_address")
    # 更换launch_id，received_date名，避免与launch表冲突
    # s筛选billing_user_count>=5的account_id
    # info_df = info_df.filter(F.col("billing_multi_shipping_count")>=10)
    # 解决冲突
    info_df = info_df.withColumnRenamed("reason", "address_reason")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            info_df,
            (F.col("UPMID") == F.col("account_id")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的billing_address_with_multiple_shipping_address打标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        "billing_address",
        "shipping_address",
        "billing_multi_shipping_count",
        "score",
        "address_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # (1)billing_info_with_multiple_account--1127
    # billing_address_with_multiple_shipping_address_upmid_df = df.filter(F.array_contains("reason", "billing_address_with_multiple_shipping_address"))
    # return billing_address_with_multiple_shipping_address_upmid_df

    billing_address_with_multiple_shipping_address_upmid_df = df.filter(
        F.array_contains("reason", "billing_address_with_multiple_shipping_address"))
    flag_upmid = billing_address_with_multiple_shipping_address_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中billing_address_with_multiple_shipping_address标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中billing_address_with_multiple_shipping_address标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中billing_address_with_multiple_shipping_address标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中billing_address_with_multiple_shipping_address标签与valid的比例:{rato_flag_in_valid}")
    return billing_address_with_multiple_shipping_address_upmid_df


# 8.get_user_with_multiple_account_df
def get_user_with_multiple_account_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F
    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    info_df = spark.table("airbot_prod.suspicious_forter_account").filter(F.col("name_user_count") >= 5)
    print("flag详情表可查询：airbot_prod.suspicious_forter_account")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # info_df = spark.table("airbot_prod.suspicious_forter_account")
    # 更换launch_id，received_date名，避免与launch表冲突
    # name_user_count>=5的account_id
    # info_df = info_df.filter(F.col("name_user_count")>=5)
    # 解决冲突
    info_df = info_df.withColumnRenamed("reason", "user_reason")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            info_df,
            (F.col("UPMID") == F.col("account_id")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的user_with_multiple_account打标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        # "SHIPPING_ADDRESS_COUNTRY",
        # "SHIPPING_ADDRESS_CITY",
        # "SHIPPING_ADDRESS_ADDRESS1",
        # "SHIPPING_ADDRESS_ADDRESS2",
        # "kr_community_id",
        "first_name",
        "last_name",

        "dob",
        # "billing_phone",
        "name_user_count",
        "score",
        "user_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # (1)billing_info_with_multiple_account--1127
    # user_with_multiple_account_upmid_df = df.filter(F.array_contains("reason", "user_with_multiple_account"))
    # return user_with_multiple_account_upmid_df

    user_with_multiple_account_upmid_df = df.filter(F.array_contains("reason", "user_with_multiple_account"))
    flag_upmid = user_with_multiple_account_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中user_with_multiple_account标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中user_with_multiple_account标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中user_with_multiple_account标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中user_with_multiple_account标签与valid的比例:{rato_flag_in_valid}")
    return user_with_multiple_account_upmid_df


# 9.get_account_with_multiple_address_df
def get_account_with_multiple_address_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F
    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )
    # 处理位置参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理位置参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )
    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    account_df = spark.table("airbot_prod.suspicious_forter_address").filter(F.col("user_multi_address_count") >= 10)
    print("flag详情表可查询：airbot_prod.suspicious_forter_address")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")

    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # account_df = spark.table("airbot_prod.suspicious_forter_address")
    # 更换launch_id，received_date名，避免与launch表冲突
    # s筛选user_multi_address_count>=10的account_id
    # account_df = account_df.filter(F.col("user_multi_address_count")>=10)
    # 解决冲突
    account_df = account_df.withColumnRenamed("reason", "address_reason")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            account_df,
            (F.col("UPMID") == F.col("account_id")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的account_with_multiple_address打标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        # "ENTRY_STATUS",
        # "billing_address",
        "shipping_address",
        "user_multi_address_count",
        "score",
        "address_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # (1)billing_info_with_multiple_account--1127
    # account_with_multiple_address_upmid_df = df.filter(F.array_contains("reason", "account_with_multiple_address"))
    # return account_with_multiple_address_upmid_df

    account_with_multiple_address_upmid_df = df.filter(F.array_contains("reason", "account_with_multiple_address"))
    flag_upmid = account_with_multiple_address_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中account_with_multiple_address标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中account_with_multiple_address标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中account_with_multiple_address标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中account_with_multiple_address标签与valid的比例:{rato_flag_in_valid}")
    return account_with_multiple_address_upmid_df


# 10.get_word_count_df
# 英文地址中，地址用空格切开出来的单词，然后算这个单词在同一个launch中用了多少次
def get_word_count_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F
    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理位置参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    pattern_df = spark.table("airbot_prod.address_bot_pattern")
    print("flag详情表可查询：airbot_prod.address_bot_pattern")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")

    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # 避免冲突
    df = df.withColumnRenamed("SHIPPING_ADDRESS_COUNTRY", "launch_SHIPPING_ADDRESS_COUNTRY").withColumnRenamed(
        "SHIPPING_ADDRESS_CITY", "launch_SHIPPING_ADDRESS_CITY").withColumnRenamed("SHIPPING_ADDRESS_ADDRESS1",
                                                                                   "launch_SHIPPING_ADDRESS_ADDRESS1").withColumnRenamed(
        "SHIPPING_ADDRESS_ADDRESS2", "launch_SHIPPING_ADDRESS_ADDRESS2")

    # pattern_df = spark.table("airbot_prod.address_bot_pattern")
    # 更换launch_id，received_date名，避免与launch表冲突
    pattern_df = pattern_df.withColumnRenamed("LAUNCH_ID", "pattern_launch_id").withColumnRenamed("received_date",
                                                                                                  "pattern_received_date").withColumnRenamed(
        "UPMID", "pattern_upmid").withColumnRenamed("reason", "pattern_reason")
    pattern_df = pattern_df.filter(F.array_contains(F.col("reason"), "word_count"))

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            pattern_df,
            (F.col("LAUNCH_ID") == F.col("pattern_launch_id")) & (F.col("UPMID") == F.col("pattern_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的risk_bot打word_count标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        "launch_SHIPPING_ADDRESS_COUNTRY",
        "launch_SHIPPING_ADDRESS_CITY",
        "launch_SHIPPING_ADDRESS_ADDRESS1",
        "launch_SHIPPING_ADDRESS_ADDRESS2",

        # "SHIPPING_ADDRESS_COUNTRY",
        # "SHIPPING_ADDRESS_CITY",
        # "SHIPPING_ADDRESS_ADDRESS1",
        # "SHIPPING_ADDRESS_ADDRESS2",
        # "community_id",
        # "kr_community_id",
        "pattern_reason",
        "score",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # word_count_upmid_df = df.filter(F.array_contains("reason", "word_count"))
    # return word_count_upmid_df

    word_count_upmid_df = df.filter(F.array_contains("reason", "word_count"))
    flag_upmid = word_count_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中word_count标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中word_count标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中word_count标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中word_count标签与valid的比例:{rato_flag_in_valid}")
    return word_count_upmid_df


# 11.get_non_standard_address_df
def get_non_standard_address_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F
    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    pattern_df = spark.table("airbot_prod.address_bot_pattern")
    print("flag详情表可查询：airbot_prod.address_bot_pattern")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # 避免冲突
    df = df.withColumnRenamed("SHIPPING_ADDRESS_COUNTRY", "launch_SHIPPING_ADDRESS_COUNTRY").withColumnRenamed(
        "SHIPPING_ADDRESS_CITY", "launch_SHIPPING_ADDRESS_CITY").withColumnRenamed("SHIPPING_ADDRESS_ADDRESS1",
                                                                                   "launch_SHIPPING_ADDRESS_ADDRESS1").withColumnRenamed(
        "SHIPPING_ADDRESS_ADDRESS2", "launch_SHIPPING_ADDRESS_ADDRESS2")

    # pattern_df = spark.table("airbot_prod.address_bot_pattern")
    # 更换launch_id，received_date名，避免与launch表冲突
    pattern_df = pattern_df.withColumnRenamed("LAUNCH_ID", "pattern_launch_id").withColumnRenamed("received_date",
                                                                                                  "pattern_received_date").withColumnRenamed(
        "UPMID", "pattern_upmid").withColumnRenamed("reason", "pattern_reason")
    pattern_df = pattern_df.filter(F.array_contains(F.col("reason"), "non_standard_address"))

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            pattern_df,
            (F.col("LAUNCH_ID") == F.col("pattern_launch_id")) & (F.col("UPMID") == F.col("pattern_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的risk_bot打non_standard_address标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        "launch_SHIPPING_ADDRESS_COUNTRY",
        "launch_SHIPPING_ADDRESS_CITY",
        "launch_SHIPPING_ADDRESS_ADDRESS1",
        "launch_SHIPPING_ADDRESS_ADDRESS2",

        # "SHIPPING_ADDRESS_COUNTRY",
        # "SHIPPING_ADDRESS_CITY",
        # "SHIPPING_ADDRESS_ADDRESS1",
        # "SHIPPING_ADDRESS_ADDRESS2",
        # "community_id",
        # "kr_community_id",
        "pattern_reason",
        "score",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # non_standard_address_upmid_df = df.filter(F.array_contains("reason", "non_standard_address"))
    # return non_standard_address_upmid_df

    non_standard_address_upmid_df = df.filter(F.array_contains("reason", "non_standard_address"))
    flag_upmid = non_standard_address_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中non_standard_address标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中non_standard_address标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中non_standard_address标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中non_standard_address标签与valid的比例:{rato_flag_in_valid}")
    return non_standard_address_upmid_df


def get_suspicious_community_jp_df(launch_id, winner=None):
    # 筛选包含suspicious_community_kr的suspicious_community_kr
    from pyspark.sql import functions as F

    # 抽出launch_id的前三个字符
    tag = launch_id[:3]

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数winner
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    # if threshold is not None:
    #   risk_df = risk_df.filter(
    #     (F.col("score")>=threshold)
    #   )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot,airbot_prod.kr_community
    # pattern_df = spark.table("airbot_prod.address_bot_pattern")
    # print("flag详情表可查询：airbot_prod.address_bot_pattern")
    pattern_df = spark.table("airbot_prod.jp_community").filter(F.col("community_id").isNotNull())
    print("flag详情表可查询：airbot_prod.address_bot_pattern & airbot_prod.jp_community")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()

    if winner is not None:
        print(f"launch_{tag}中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch_{tag}中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    # print("Filter out suspicious through the member_risk_score_exclude_demand table")
    # susp_upmid=df.count()
    # if winner is not None:
    #     print(f"launch_{tag}中winner_upmid中exclude_human_suspicious数量:{susp_upmid} (non_threshold)")
    # else:
    #     # valid_upmid=launch_df.count()
    #     print(f"launch_{tag}中valid_upmid中exclude_human_suspicious数量:{susp_upmid} (non_threshold)")

    # 打reason_flag

    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")

    # risk_upmid=df.count()
    # if winner is not None:
    #     print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    # else:
    #     # valid_upmid=launch_df.count()
    #     print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # non_threshold
    risk_upmid = df.count()
    # threshold_35
    risk_upmid_threshold_35 = df.filter(F.col("score") >= 35).count()

    if winner is not None:
        print(f"launch_{tag}中winner_upmid中risk_reason标签的数量:{risk_upmid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中risk_reason标签的数量:{risk_upmid_threshold_35} (threshold_35)")

    else:
        # valid_upmid=launch_df.count()
        print(f"launch_{tag}中valid_upmid中risk_reason标签的数量:{risk_upmid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中risk_reason标签的数量:{risk_upmid_threshold_35} (threshold_35)")

    # 避免冲突
    df = df.withColumnRenamed("SHIPPING_ADDRESS_COUNTRY", "launch_SHIPPING_ADDRESS_COUNTRY").withColumnRenamed(
        "SHIPPING_ADDRESS_CITY", "launch_SHIPPING_ADDRESS_CITY").withColumnRenamed("SHIPPING_ADDRESS_ADDRESS1",
                                                                                   "launch_SHIPPING_ADDRESS_ADDRESS1").withColumnRenamed(
        "SHIPPING_ADDRESS_ADDRESS2", "launch_SHIPPING_ADDRESS_ADDRESS2")

    # 更换launch_id，received_date名，避免与launch表冲突
    pattern_df = pattern_df.withColumnRenamed("forter_address__account_id_list_for_the_address", "pattern_upmid")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            pattern_df,
            (F.col("UPMID") == F.col("pattern_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    # print(f"给launch表的upimd中打对应的risk_bot打kr_coummunity_id标签的数量:{df.count()}")

    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        "launch_SHIPPING_ADDRESS_COUNTRY",
        "launch_SHIPPING_ADDRESS_CITY",
        "launch_SHIPPING_ADDRESS_ADDRESS1",
        "launch_SHIPPING_ADDRESS_ADDRESS2",

        # "SHIPPING_ADDRESS_COUNTRY",
        # "SHIPPING_ADDRESS_CITY",
        # "SHIPPING_ADDRESS_ADDRESS1",
        # "SHIPPING_ADDRESS_ADDRESS2",
        "address",
        "community_id",
        "score",
        # "pattern_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # (1)suspicious_community_kr 4198

    suspicious_community_jp_upmid_df = df.filter(F.array_contains("reason", "suspicious_community_jp"))
    # non_threshold
    flag_upmid = suspicious_community_jp_upmid_df.count()
    # threshold_35
    flag_upmid_threshold_35 = suspicious_community_jp_upmid_df.filter(F.col("score") >= 35).count()
    if winner is not None:
        print(f"launch_{tag}中winner_upmid中suspicious_community_jp标签的数量:{flag_upmid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中suspicious_community_jp标签的数量:{flag_upmid_threshold_35} (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中suspicious_community_jp标签的数量:{flag_upmid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中suspicious_community_jp标签的数量:{flag_upmid_threshold_35} (threshold_35)")

    # non_threshold
    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    # threshold_35
    rato_flag_in_valid_35 = round((flag_upmid_threshold_35 / valid_upmid), 4)

    if winner is not None:
        print(f"launch_{tag}中winner_upmid中suspicious_community_jp标签与winner的比例:{rato_flag_in_valid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中suspicious_community_jp标签与winner的比例:{rato_flag_in_valid_35} (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中suspicious_community_jp标签与valid的比例:{rato_flag_in_valid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中suspicious_community_jp标签与valid的比例:{rato_flag_in_valid_35} (threshold_35)")

    print("END!!! return suspicious_community_jp_upmid_df (non_threshold)")
    return suspicious_community_jp_upmid_df


# 7.get_billing_address_with_multiple_shipping_address_df
def get_billing_info_with_multiple_shipping_address_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    info_df = spark.table("airbot_prod.suspicious_forter_address").filter(
        F.col("billing_info_multi_address_count") >= 10)
    print("flag详情表可查询：airbot_prod.suspicious_forter_address")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")

    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{sups_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason数量:{risk_upmid}")

    # info_df = spark.table("airbot_prod.suspicious_forter_address")
    # 更换launch_id，received_date名，避免与launch表冲突
    # s筛选billing_user_count>=5的account_id
    # info_df = info_df.filter(F.col("billing_multi_shipping_count")>=10)
    # 解决冲突
    info_df = info_df.withColumnRenamed("reason", "address_reason")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            info_df,
            (F.col("UPMID") == F.col("account_id")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的billing_info_with_multiple_shipping_address打标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        # "billing_address",
        # "shipping_address",
        # "billing_multi_shipping_count",
        "billing_phone",
        "last4",
        "billing_info_multi_address_count",
        "score",
        "address_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # (1)billing_info_with_multiple_account--1127
    # billing_info_with_multiple_shipping_address_upmid_df = df.filter(F.array_contains("reason", "billing_info_with_multiple_shipping_address"))
    # return billing_info_with_multiple_shipping_address_upmid_df

    billing_info_with_multiple_shipping_address_upmid_df = df.filter(
        F.array_contains("reason", "billing_info_with_multiple_shipping_address"))

    flag_upmid = billing_info_with_multiple_shipping_address_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中billing_info_with_multiple_shipping_address标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中billing_info_with_multiple_shipping_address标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中billing_info_with_multiple_shipping_address标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中billing_info_with_multiple_shipping_address标签与valid的比例:{rato_flag_in_valid}")
    return billing_info_with_multiple_shipping_address_upmid_df


# forter_community
def get_forter_community_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 抽出launch_id的前三个字符
    tag = launch_id[:3]

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    # if threshold is not None:
    #   risk_df = risk_df.filter(
    #     (F.col("score")>=threshold)
    #   )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    forter_community_df = spark.table("airbot_prod.forter_community_score").filter(
        (F.col("score") >= 0.3)
    )
    print("flag详情表可查询：airbot_prod.forter_community_score")
    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch_{tag}中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch_{tag}中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    # susp_upmid=df.count()
    # susp_upmid_35=df.filter(F.col("score")>=35).count()
    # if winner is not None:
    #   print(f"launch中winner_upmid中suspicious数量:{susp_upmid} (non_threshold)")
    #   print(f"launch中winner_upmid中suspicious数量:{susp_upmid_35} (threshold_35)")
    # else:
    #   # 打标签--此处我只要bot,故而只需要inner
    #   print(f"launch中valid_upmid中suspicious数量:{susp_upmid} (non_threshold)")
    #   print(f"launch中valid_upmid中suspicious数量:{susp_upmid_35} (threshold_35)")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    risk_upmid_35 = df.filter(F.col("score") >= 35).count()

    if winner is not None:
        print(f"launch_{tag}中winner_upmid中risk_reason数量:{risk_upmid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中risk_reason数量:{risk_upmid_35} (threshold_35)")
    else:
        # 打标签--此处我只要bot,故而只需要inner
        print(f"launch_{tag}中valid_upmid中risk_reason数量:{risk_upmid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中risk_reason数量:{risk_upmid_35} (threshold_35)")

    forter_community_df = forter_community_df.withColumnRenamed("upmid", "forter_upmid").withColumnRenamed("score",
                                                                                                           "forter_score")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            forter_community_df,
            (F.col("UPMID") == F.col("forter_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中forter_community打标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        "address",
        "community_id",
        "distinct_upm_count",
        "all_upm_count",
        "forter_score",
        "score",
        "reason",
        "received_date"
    )
    # 防止一个launch中的upmid可能没有

    forter_community_upmid_df = df.filter(F.array_contains("reason", "forter_community"))

    flag_upmid = forter_community_upmid_df.count()
    flag_upmid_35 = forter_community_upmid_df.filter(F.col("score") >= 35).count()
    if winner is not None:
        print(f"launch_{tag}中winner_upmid中forter_community标签的数量:{flag_upmid}  (non_threshold)")
        print(f"launch_{tag}中winner_upmid中forter_community标签的数量:{flag_upmid_35}  (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中forter_community标签的数量:{flag_upmid}  (non_threshold)")
        print(f"launch_{tag}中valid_upmid中forter_community标签的数量:{flag_upmid_35}  (threshold_35)")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    rato_flag_in_valid_35 = round((flag_upmid_35 / valid_upmid), 4)
    if winner is not None:
        print(f"launch_{tag}中winner_upmid中forter_community标签与winner的比例:{rato_flag_in_valid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中forter_community标签与winner的比例:{rato_flag_in_valid_35} (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中forter_community标签与valid的比例:{rato_flag_in_valid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中forter_community标签与valid的比例:{rato_flag_in_valid_35} (threshold_35)")
    print("END!!! return forter_community_upmid_df (non_threshold)")
    return forter_community_upmid_df


# community---主要再美国比较多
def get_community_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 抽出launch_id的前三个字符
    tag = launch_id[:3]

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    # if threshold is not None:
    #   risk_df = risk_df.filter(
    #     (F.col("score")>=threshold)
    #   )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    community_df = spark.table("airbot_prod.community_score")
    print("flag详情表可查询：airbot_prod.community_score")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch_{tag}中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch_{tag}中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")
    # susp_upmid=df.count()
    # if winner is not None:
    #   print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    # else:
    #   print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    risk_upmid_35 = df.filter(F.col("score") >= 35).count()

    if winner is not None:
        print(f"launch_{tag}中winner_upmid中risk_reason标签的数量:{risk_upmid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中risk_reason标签的数量:{risk_upmid_35} (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中risk_reason标签的数量:{risk_upmid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中risk_reason标签的数量:{risk_upmid_35} (threshold_35)")

    community_df = community_df.withColumnRenamed("upmid", "community_upmid").withColumnRenamed("score",
                                                                                                "community_score")

    #  filtered_df是左表，highheat_df是右表
    joined_df = (
        df.join(
            community_df,
            (F.col("UPMID") == F.col("community_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的community打标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        "community_id",
        "address",
        "SHIPPING_ADDRESS_COUNTRY",
        "SHIPPING_ADDRESS_CITY",
        "SHIPPING_ADDRESS_ADDRESS1",
        "SHIPPING_ADDRESS_ADDRESS2",
        "score",
        "reason",
        "received_date"
    )
    # 防止一个launch中的upmid可能没有

    community_upmid_df = df.filter(F.array_contains("reason", "community"))
    flag_upmid = community_upmid_df.count()
    flag_upmid_35 = community_upmid_df.filter(F.col("score") >= 35).count()
    if winner is not None:
        print(f"launch_{tag}中winner_upmid中community标签的数量:{flag_upmid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中community标签的数量:{flag_upmid_35} (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中community标签的数量:{flag_upmid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中community标签的数量:{flag_upmid_35} (threshold_35)")
    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    rato_flag_in_valid_35 = round((flag_upmid_35 / valid_upmid), 4)
    if winner is not None:
        print(f"launch_{tag}中winner_upmid中community标签与winner的比例:{rato_flag_in_valid} (non_threshold)")
        print(f"launch_{tag}中winner_upmid中community标签与winner的比例:{rato_flag_in_valid_35} (threshold_35)")
    else:
        print(f"launch_{tag}中valid_upmid中community标签与valid的比例:{rato_flag_in_valid} (non_threshold)")
        print(f"launch_{tag}中valid_upmid中community标签与valid的比例:{rato_flag_in_valid_35} (threshold_35)")
    print("END!!! return community_upmid_df (non_threshold)")
    return community_upmid_df


def get_change_address_frequently_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )
    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    pattern_df = spark.table("airbot_prod.address_bot_pattern")
    print("flag详情表可查询：airbot_prod.address_bot_pattern")

    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")
    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"使用risk_bot给launch表打bot标签的数量:{df.count()}")
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    # 避免冲突
    df = df.withColumnRenamed("SHIPPING_ADDRESS_COUNTRY", "launch_SHIPPING_ADDRESS_COUNTRY").withColumnRenamed(
        "SHIPPING_ADDRESS_CITY", "launch_SHIPPING_ADDRESS_CITY").withColumnRenamed("SHIPPING_ADDRESS_ADDRESS1",
                                                                                   "launch_SHIPPING_ADDRESS_ADDRESS1").withColumnRenamed(
        "SHIPPING_ADDRESS_ADDRESS2", "launch_SHIPPING_ADDRESS_ADDRESS2")

    # 更换launch_id，received_date名，避免与launch表冲突
    pattern_df = pattern_df.withColumnRenamed("LAUNCH_ID", "pattern_launch_id").withColumnRenamed("received_date",
                                                                                                  "pattern_received_date").withColumnRenamed(
        "UPMID", "pattern_upmid").withColumnRenamed("reason", "pattern_reason")
    pattern_df = pattern_df.filter(F.array_contains(F.col("reason"), "change_address_frequently"))

    joined_df = (
        df.join(
            pattern_df,
            (F.col("LAUNCH_ID") == F.col("pattern_launch_id")) & (F.col("UPMID") == F.col("pattern_upmid")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd中打对应的risk_bot打change_address_frequently标签的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        "score",
        "pattern_reason",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    change_address_frequently_upmid_df = df.filter(F.array_contains("reason", "change_address_frequently"))

    return change_address_frequently_upmid_df

    change_address_frequently_upmid_df = df.filter(F.array_contains("reason", "change_address_frequently"))
    flag_upmid = change_address_frequently_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中change_address_frequently标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中change_address_frequently标签的数量:{flag_upmid}")
    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中change_address_frequently标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中change_address_frequently标签与valid的比例:{rato_flag_in_valid}")
    return change_address_frequently_upmid_df


def get_forter_email_df(launch_id, winner=None, threshold=None):
    from pyspark.sql import functions as F

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )
    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 处理关键词参数
    if threshold is not None:
        risk_df = risk_df.filter(
            (F.col("score") >= threshold)
        )

    # 通过demand的score>0过滤掉launch中中的human_flag，只留下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    pattern_df = spark.table("airbot_prod.suspicious_email_account")
    print("flag详情表可查询：airbot_prod.suspicious_email_account")
    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")
    susp_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")
    risk_upmid = df.count()
    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid}")

    joined_df = (
        df.join(
            pattern_df,
            (F.col("UPMID") == F.col("account_id")),
            how="left"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        "lower_case",
        "ship_email",
        "ship_country",
        "email_user_count",
        "score",
        "reason",
        "received_date"
    )

    # 挑选出数量较多的flag的的upmid
    # forter_email_upmid_df = df.filter(F.array_contains("reason", "forter_email"))
    # print(f"launch中valid_upmid中forter_email标签的数量:{forter_email_upmid_df.count()}")
    # return forter_email_upmid_df

    forter_email_upmid_df = df.filter(F.array_contains("reason", "forter_email"))
    flag_upmid = forter_email_upmid_df.count()
    if winner is not None:
        print(f"launch中winner_upmid中forter_email标签的数量:{flag_upmid}")
    else:
        print(f"launch中valid_upmid中forter_email标签的数量:{flag_upmid}")

    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    if winner is not None:
        print(f"launch中winner_upmid中forter_email标签与winner的比例:{rato_flag_in_valid}")
    else:
        print(f"launch中valid_upmid中forter_email标签与valid的比例:{rato_flag_in_valid}")
    return forter_email_upmid_df

# %run
# /Users/Yueyang.Chen@nike.com/common/sf_util
# 1.get_session_flag_detail_df
# 同一会话有超过 10 个不同帐户登陆&同一个会话创建超过2个账户
# 没有threshold的时候，也可以计算非thrashold和threshold_35
def get_session_flag_df(launch_id, winner=None, threshold=None):
    # 筛选包含suspicious_community_kr的suspicious_community_kr
    from pyspark.sql import functions as F
    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("launch_id") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    )

    # 处理关键词参数
    if winner is not None:
        launch_df = launch_df.filter(
            (F.col("ENTRY_STATUS") == winner)
        )

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # # 处理关键词参数
    # if threshold is not None:
    #   risk_df = risk_df.filter(
    #     (F.col("score")>=threshold)
    #   )

    # 通过demand的score>0过滤掉human_flag，只剩下suspicious
    susp_df = spark.table("airbot_prod.member_risk_score_exclude_demand").filter(
        (F.col("score") > 0)
    ).withColumnRenamed("identity_upm", "susp_upm").withColumnRenamed("score", "susp_score").withColumnRenamed("desc",
                                                                                                               "susp_desc").dropDuplicates(
        ["susp_upm"])

    # 从具体详情表中筛选bot
    session_df = read_from_sf('select * from nd_cde_prod.stg_bot.session_flag')
    print("flag详情表可查询：select * from nd_cde_prod.stg_bot.session_flag")
    launch_df = launch_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    valid_upmid = launch_df.count()
    if winner is not None:
        print(f"launch中winner_upmid数量:{valid_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid数量:{valid_upmid}")

    # 过滤掉human，只留下suspicious
    joined_df = (
        launch_df.join(
            susp_df,
            (F.col("UPMID") == F.col("susp_upm")),
            how="inner"
        )
    )
    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid中suspicious数量:{df.count()}")
    susp_upmid = df.count()

    if winner is not None:
        print(f"launch中winner_upmid中suspicious数量:{susp_upmid}")
    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中suspicious数量:{susp_upmid}")

    # 打标签--此处我只要bot,故而只需要inner
    joined_df = (
        df.join(
            risk_df,
            (F.col("UPMID") == F.col("identity_upm")),
            how="inner"
        )
    )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"launch中valid_upmid数量:{df.count()}")
    # print(f"launch中valid_upmid中risk_reason标签的数量:{df.count()}")

    # non_threshold
    risk_upmid = df.count()
    # threshold_35
    risk_upmid_threshold_35 = df.filter(F.col("score") >= 35).count()

    if winner is not None:
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid} (non_threshold)")
        print(f"launch中winner_upmid中risk_reason标签的数量:{risk_upmid_threshold_35} (non_threshold)")

    else:
        # valid_upmid=launch_df.count()
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid} (non_threshold)")
        print(f"launch中valid_upmid中risk_reason标签的数量:{risk_upmid_threshold_35} (threshold_35)")

    # 避免冲突
    df = df.withColumnRenamed("SHIPPING_ADDRESS_COUNTRY", "launch_SHIPPING_ADDRESS_COUNTRY").withColumnRenamed(
        "SHIPPING_ADDRESS_CITY", "launch_SHIPPING_ADDRESS_CITY").withColumnRenamed("SHIPPING_ADDRESS_ADDRESS1",
                                                                                   "launch_SHIPPING_ADDRESS_ADDRESS1").withColumnRenamed(
        "SHIPPING_ADDRESS_ADDRESS2", "launch_SHIPPING_ADDRESS_ADDRESS2").withColumnRenamed("SESSION_FLAG",
                                                                                           "launch_SESSION_FLAG")

    joined_df = (
        df.join(
            session_df,
            (F.col("UPMID") == F.col("CONSUMER_UPM_ID")),
            how="left"
        )
    )

    # joined_df = (
    #       joined_df.join(
    #         susp_df,
    #         (F.col("UPMID")==F.col("susp_upm")),
    #         how="inner"
    #       )
    # )

    df = joined_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])
    # print(f"给launch表的upimd打session_flag标签的upmid的数量:{df.count()}")
    # 筛选重视的字段
    df = df.select(
        "LAUNCH_ID",
        "UPMID",
        #   "ENTRY_STATUS",
        "NIKEDOTCOM_MULTI_LOGIN_FLAG",
        "NIKEDOTCOM_MULTI_REGISTRATION_FLAG",

        "NIKEAPP_MULTI_LOGIN_FLAG",
        "NIKEAPP_MULTI_REGISTRATION_FLAG",

        "SNKRS_MULTI_LOGIN_FLAG",
        "SNKRS_MULTI_REGISTRATION_FLAG",
        "SESSION_FLAG",
        "score",
        "reason",
        "received_date"
    )
    # df.createOrReplaceTempView("temp_of_launch_6d6_L1_human_in_risk_bot_upmid")
    # session_flag_upmid_df = df.filter(F.array_contains("reason", "session_flag"))
    # 要过滤特定值的数组列，使用～array_contains。所以只需要～F.array_contains
    # return session_flag_upmid_df

    session_flag_upmid_df = df.filter(F.array_contains("reason", "session_flag"))
    # non_threshold
    flag_upmid = session_flag_upmid_df.count()
    # threshold_35
    flag_upmid_threshold_35 = session_flag_upmid_df.filter(F.col("score") >= 35)
    if winner is not None:
        print(f"launch中winner_upmid中session_flag标签的数量:{flag_upmid} (non_threshold)")
        print(f"launch中winner_upmid中session_flag标签的数量:{flag_upmid_threshold_35} (threshold_35)")
    else:
        print(f"launch中valid_upmid中session_flag标签的数量:{flag_upmid} (non_threshold)")
        print(f"launch中vaild_upmid中session_flag标签的数量:{flag_upmid_threshold_35} (threshold_35)")
    # non_threshold
    rato_flag_in_valid = round((flag_upmid / valid_upmid), 4)
    # threshold_35

    if winner is not None:
        print(f"launch中winner_upmid中session_flag标签与winner的比例:{rato_flag_in_valid} (non_threshold)")
    else:
        print(f"launch中valid_upmid中session_flag标签与valid的比例:{rato_flag_in_valid} (non_threshold)")
    print("-----------END!!! return session_flag_upmid_df (non_threshold)-----------")
    return session_flag_upmid_df
