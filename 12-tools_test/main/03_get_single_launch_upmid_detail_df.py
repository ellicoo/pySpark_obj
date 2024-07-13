# 没上模型之前
def get_non_pattern_single_launch_upmid_detail_df(launch_id, winner=None):
    from pyspark.sql import functions as F

    # 更新表：
    spark.sql("refresh table launch_secure.launch_entries")
    spark.sql("refresh table airbot_prod.bot_master_all_risk_level_score")
    spark.sql("refresh table swatbots_secure.heuristics_predictions")
    spark.sql("refresh table airbot_prod.member_risk_score_exclude_demand")
    spark.sql("refresh table swatbots_secure.ml_predictions_launch_bots")

    # 抽出launch_id的前三个字符
    tag = launch_id[:3]
    if winner is not None:
        print(f"**************launch_{tag}_winner statistical details***************")
    else:
        print(f"**************launch_{tag}_valid statistical details***************")

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("LAUNCH_ID") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    ).dropDuplicates(["LAUNCH_ID", "UPMID"])

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 对risk_reason的统计需要
    # 通过demand的score>0过滤掉launch中的human_flag，只留下suspicious
    # susp_df=spark.table("airbot_prod.member_risk_score_exclude_demand").withColumnRenamed("identity_upm","susp_upm").withColumnRenamed("score","score_exclude_demand").withColumnRenamed("desc","susp_desc").dropDuplicates(["susp_upm"])

    # single_join
    # 进行规则表的打标签：没上模型时
    # 模型进行打标签：swatbots_secure.themis_predictions
    predic_df = spark.table("swatbots_secure.heuristics_predictions").withColumnRenamed("upmid", "predic_upmid").filter(
        (F.col("launchid") == launch_id)
    ).dropDuplicates(["predic_upmid"])
    # 此处如果winner！=None则launch_df已经filter出winner
    # launch_upmid打规则表的标签，
    predic_df = launch_df.join(
        predic_df,
        (F.col("UPMID") == F.col("predic_upmid")) & (F.col("launchid") == F.col("LAUNCH_ID")),
        how="left"
    )

    predic_df = predic_df.dropDuplicates(["LAUNCH_ID", "UPMID"])
    predic_nums = predic_df.filter(F.col("heuristics_is_human") == 0).count()

    # double_join：'single_launch' join 'all_risk' join 'susp_df'即airbot_prod.member_risk_score_exclude_demand的score>0
    # 看launch中，risk_reason的数量：
    # 因为risk包含所有的upmid，此处可以使用inner

    joined_risk_df = predic_df.join(
        risk_df,
        (F.col("UPMID") == F.col("identity_upm")),
        how="inner"
    )

    # 去重,因为VALIDATION_RESULT都是valid，所以去重可以包含
    joined_risk_df = joined_risk_df.dropDuplicates(["LAUNCH_ID", "UPMID"])

    # 创建新列，并将operational_tier值作为列的常量值
    # new_df = joined_risk_df.withColumn("operational_tier", F.lit(operational_tier_value))

    # 假设上模型
    # select * from swatbots_secure.ml_predictions_launch_bots
    # 判断规则F.col("is_bot")==1
    ml_predic_df = spark.table("swatbots_secure.ml_predictions_launch_bots").dropDuplicates(
        ["launch_id", "upmid"]).withColumnRenamed("launch_id", "ml_launch_id").withColumnRenamed("upmid",
                                                                                                 "ml_upmid").withColumnRenamed(
        "received_date", "ml_received_data").withColumnRenamed("is_bot", "ml_bot")

    joined_ml_df = (
        joined_risk_df.join(
            ml_predic_df,
            (F.col("UPMID") == F.col("ml_upmid")) & (F.col("ml_launch_id") == F.col("LAUNCH_ID")),
            how="left"
        )
    )

    # joined_df = joined_df.dropDuplicates(["UPMID", "VALIDATION_RESULT"])
    joined_ml_df = joined_ml_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    # 过滤掉human，只留下suspicious,但保留launch的所有upmid
    # susp_df = (
    #   joined_ml_df.join(
    #     susp_df,
    #     (F.col("UPMID")==F.col("susp_upm")),
    #     how="left"
    #     )
    #   )
    # susp_df=susp_df.dropDuplicates(["LAUNCH_ID","UPMID","VALIDATION_RESULT"])

    # 此时完成三表连接：join，筛选需要的字段
    susp_df = joined_ml_df.select(
        "LAUNCH_ID",
        # "operational_tier",
        "UPMID",
        "PRODUCT_MERCH_GROUP",
        # "PRODUCT_COUNTRY",
        "VALIDATION_RESULT",
        "ENTRY_STATUS",
        # 标签来自规则表
        "heuristics_is_human",
        # 标签来自swatbots_secure.ml_predictions_launch_bots
        "ml_bot",
        # 标签来自airbot_prod.member_risk_score_exclude_demand 需要score_exclude_demand>0才能过滤human_flag
        # "score_exclude_demand",
        # 以下全部来自大的risk表
        "score_w_human_exclude_demand",
        "score_w_human",
        "score",
        "reason",
        # 来自launch
        "received_date"
    )

    # 统计valid
    # 全局valid 数量
    valid_df = susp_df.filter(F.col("VALIDATION_RESULT") == "VALID")
    valid_nums = valid_df.count()

    # # 创建valid临时表
    # valid_upmid_temp_view=f"launch_{tag}_valid_upmid_detail_temp_view"
    # valid_df.createOrReplaceTempView(valid_upmid_temp_view)

    # winner/valid
    # print(f"winner/valid:{round(winner_nums/valid_nums,4)}")

    if winner is not None:
        # 统计winner
        # winner数量
        winner_df = susp_df.filter(F.col("ENTRY_STATUS") == "WINNER")
        winner_nums = winner_df.count()
        # 创建winner临时表
        winner_upmid_temp_view = f"launch_{tag}_winner_upmid_detail_temp_view"
        winner_df.createOrReplaceTempView(winner_upmid_temp_view)

        # winner risk
        # 无阈值
        winner_suspicous_df = winner_df.filter(F.col("score_w_human_exclude_demand") > 0)
        winner_suspicous_nums = winner_suspicous_df.count()
        # 计算group_reason 排名
        winner_group_reason_df = winner_suspicous_df.groupBy("reason").agg(F.count("*").alias("count")).orderBy(
            F.desc("count"))
        winner_group_reason_view = f"launch_{tag}_winner_non_threshold_group_reason_ranking_temp_view"
        winner_group_reason_df.createOrReplaceTempView(winner_group_reason_view)
        # 计算单个reason排名
        winner_single_reason_df = winner_suspicous_df.withColumn("single_reason", F.explode(F.col("reason"))).groupBy(
            "single_reason").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
        winner_single_reason_view = f"launch_{tag}_winner_non_threshold_single_reason_ranking_temp_view"
        winner_single_reason_df.createOrReplaceTempView(winner_single_reason_view)

        # 有阈值
        winner_risk_df = winner_suspicous_df.filter(F.col("score") >= 35)
        winner_risk_nums = winner_risk_df.count()
        # 计算分组reason
        winner_threshold_35_group_reason_df = winner_risk_df.groupBy("reason").agg(F.count("*").alias("count")).orderBy(
            F.desc("count"))
        winner_threshold_35_group_reason_view = f"launch_{tag}_winner_threshold_35_group_reason_ranking_temp_view"
        winner_threshold_35_group_reason_df.createOrReplaceTempView(winner_threshold_35_group_reason_view)

        # 计算单个reason
        winner_threshold_35_single_reason_df = winner_risk_df.withColumn("single_reason",
                                                                         F.explode(F.col("reason"))).groupBy(
            "single_reason").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
        winner_threshold_35_single_reason_view = f"launch_{tag}_winner_threshold_35_single_reason_ranking_temp_view"
        winner_threshold_35_single_reason_df.createOrReplaceTempView(winner_threshold_35_single_reason_view)

        # print("Winner_detail:")
        print(f"winner数量:{winner_nums}")
        print(f'winner_heuristics_bot数量:{winner_df.filter(F.col("heuristics_is_human") == 0).count()}')
        print(f'winner_ml_bot数量:{winner_df.filter(F.col("ml_bot") == 1).count()}')
        print(f"winner_suspicous_bot数量:{winner_suspicous_nums} (non_threshold)")
        print(f"winner_suspicous_bot/winner: {round(winner_suspicous_nums / winner_nums, 4)} (non_threshold)")
        print(f'winner_risk_bot数量:{winner_risk_nums} (threshold_35)')
        print(f"winner_risk_bot/winner: {round(winner_risk_nums / winner_nums, 4)} (threshold_35)")

        # print(f"----为launch_{tag}_winner_upmid创建详情表(可查询)：")
        # print(f"{winner_upmid_temp_view}")
        # winner无阈值的创建了reason排名表
        print(f"为launch_{tag}_winner创建reason排名表(可查询):")
        print(f"{winner_group_reason_view}  (non_threshold)")
        print(f"{winner_single_reason_view}  (non_threshold)")
        # winner阈值35，创建了reason排名表
        print(f"{winner_threshold_35_group_reason_view} (threshold_35)")
        print(f"{winner_threshold_35_single_reason_view} (threshold_35)")
        print(f"-----------------------------------------------")
        # winner/valid
        print(f"valid数量:{valid_nums}")
        print(f"winner/valid:{round(winner_nums / valid_nums, 4)}")
        print(f"END!!! 返回launch_{tag}_winner_upmid_detail_df")
        return winner_df

    else:
        # 统计valid
        # valid 数量
        # valid risk
        # 无阈值
        valid_suspicous_df = valid_df.filter(F.col("score_w_human_exclude_demand") > 0)
        valid_suspicous_nums = valid_suspicous_df.count()
        # 计算group_reason排名
        valid_group_reason_df = valid_suspicous_df.groupBy("reason").agg(F.count("*").alias("count")).orderBy(
            F.desc("count"))
        valid_group_reason_view = f"launch_{tag}_valid_non_threshold_group_reason_ranking_temp_view"
        valid_group_reason_df.createOrReplaceTempView(valid_group_reason_view)
        # 计算单个reason
        valid_single_reason_df = valid_suspicous_df.withColumn("single_reason", F.explode(F.col("reason"))).groupBy(
            "single_reason").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
        valid_single_reason_view = f"launch_{tag}_valid_non_threshold_single_reason_ranking_temp_view"
        valid_single_reason_df.createOrReplaceTempView(valid_single_reason_view)

        # 有阈值
        valid_risk_df = valid_suspicous_df.filter(F.col("score") >= 35)
        valid_risk_nums = valid_risk_df.count()
        # 计算分组reason
        valid_threshold_35_group_reason_df = valid_risk_df.groupBy("reason").agg(F.count("*").alias("count")).orderBy(
            F.desc("count"))
        valid_threshold_35_group_reason_view = f"launch_{tag}_valid_threshold_35_group_reason_ranking_temp_view"
        valid_threshold_35_group_reason_df.createOrReplaceTempView(valid_threshold_35_group_reason_view)

        # 计算单个reason
        valid_threshold_35_single_reason_df = valid_risk_df.withColumn("single_reason",
                                                                       F.explode(F.col("reason"))).groupBy(
            "single_reason").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
        valid_threshold_35_single_reason_view = f"launch_{tag}_valid_threshold_35_single_reason_ranking_temp_view"
        valid_threshold_35_single_reason_df.createOrReplaceTempView(valid_threshold_35_single_reason_view)

        # print("Valid_detail:")
        print(f"valid数量:{valid_nums}")
        print(f'valid_heuristics_bot数量:{valid_df.filter(F.col("heuristics_is_human") == 0).count()}')
        print(f'valid_ml_bot数量:{valid_df.filter(F.col("ml_bot") == 1).count()}')
        print(f"valid_suspicous_bot数量:{valid_suspicous_nums} (non_threshold)")
        print(f"valid_suspicous_bot/valid: {round(valid_suspicous_nums / valid_nums, 4)} (non_threshold)")
        print(f'valid_risk_bot数量:{valid_risk_nums} (threshold_35)')
        print(f"valid_risk_bot/valid: {round(valid_risk_nums / valid_nums, 4)} (threshold_35)")

        # print(f"----为launch_{tag}_valid_upmid创建详情表(可查询)：")
        # print(f"{valid_upmid_temp_view}")
        # valid无阈值的创建了reason排名表
        print(f"为launch_{tag}_valid创建reason排名表(可查询):")
        print(f"{valid_group_reason_view} (non_threshold)")
        print(f"{valid_single_reason_view} (non_threshold)")
        # valid阈值35，创建了reason排名表
        print(f"{valid_threshold_35_group_reason_view} (threshold_35)")
        print(f"{valid_threshold_35_single_reason_view} (threshold_35)")
        print(f"END!!! 返回launch_{tag}_valid_upmid_detail_df")
        return susp_df

# 上模型之后的：
def get_pattern_single_launch_upmid_detail_df(launch_id, winner=None):
    from pyspark.sql import functions as F

    # 更新表：
    spark.sql("refresh table launch_secure.launch_entries")
    spark.sql("refresh table airbot_prod.bot_master_all_risk_level_score")
    spark.sql("refresh table swatbots_secure.themis_predictions")
    spark.sql("refresh table airbot_prod.member_risk_score_exclude_demand")
    spark.sql("refresh table swatbots_secure.ml_predictions_launch_bots")
    # 抽出launch_id的前三个字符
    tag = launch_id[:3]
    if winner is not None:
        print(f"**************launch_{tag}_winner statistical details***************")
    else:
        print(f"**************launch_{tag}_valid statistical details***************")

    # 从launch表中筛选launch_id--L1_human:VALID
    launch_df = spark.table("launch_secure.launch_entries").filter(
        (F.col("LAUNCH_ID") == launch_id) &
        (F.col("VALIDATION_RESULT") == "VALID")
    ).dropDuplicates(["LAUNCH_ID", "UPMID"])

    # 从risk表中筛选bot标签
    risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").dropDuplicates(["identity_upm"])

    # 对risk_reason的统计需要
    # 通过demand的score>0过滤掉launch中的human_flag，只留下suspicious
    # susp_df=spark.table("airbot_prod.member_risk_score_exclude_demand").withColumnRenamed("identity_upm","susp_upm").withColumnRenamed("score","score_exclude_demand").withColumnRenamed("desc","susp_desc").dropDuplicates(["susp_upm"])

    # single_join
    # 进行规则表的打标签：没上模型时
    # 模型进行打标签：swatbots_secure.themis_predictions
    # predic_df = spark.table("swatbots_secure.heuristics_predictions").withColumnRenamed("upmid", "predic_upmid").filter(
    #    (F.col("launchid")==launch_id)
    #   ).dropDuplicates(["predic_upmid"])
    # # 此处如果winner！=None则launch_df已经filter出winner
    # # launch_upmid打规则表的标签，
    # predic_df = launch_df.join(
    #   predic_df,
    #   (F.col("UPMID") == F.col("predic_upmid")) & (F.col("launchid") == F.col("LAUNCH_ID")),
    #   how="left"
    # )
    predic_df = spark.table("swatbots_secure.themis_predictions").withColumnRenamed("upmid",
                                                                                    "predic_upmid").withColumnRenamed(
        "is_human", "themis_human")

    predic_df = (
        launch_df.join(
            predic_df,
            # result_with_bot_ind_h["identity_upm"] == predic_df["predic_upmid"],
            # F.col("UPMID") == F.col("predic_upmid"),
            # 注意时间匹配
            (F.col("UPMID") == F.col("predic_upmid")) & (F.col("received_date") == F.col("day")) & (
                        F.col("merch_group") == F.col("PRODUCT_MERCH_GROUP")),
            how="left"
        )
    )

    predic_df = predic_df.dropDuplicates(["LAUNCH_ID", "UPMID"])
    # predic_nums = predic_df.filter(F.col("themis_human")==0).count()

    # double_join：'single_launch' join 'all_risk' join 'susp_df'即airbot_prod.member_risk_score_exclude_demand的score>0
    # 看launch中，risk_reason的数量：
    # 因为risk包含所有的upmid，此处可以使用inner

    joined_risk_df = predic_df.join(
        risk_df,
        (F.col("UPMID") == F.col("identity_upm")),
        how="inner"
    )

    # 去重,因为VALIDATION_RESULT都是valid，所以去重可以包含
    joined_risk_df = joined_risk_df.dropDuplicates(["LAUNCH_ID", "UPMID"])

    # 创建新列，并将operational_tier值作为列的常量值
    # new_df = joined_risk_df.withColumn("operational_tier", F.lit(operational_tier_value))

    # 假设上模型
    # select * from swatbots_secure.ml_predictions_launch_bots
    # 判断规则F.col("is_bot")==1
    ml_predic_df = spark.table("swatbots_secure.ml_predictions_launch_bots").dropDuplicates(
        ["launch_id", "upmid"]).withColumnRenamed("launch_id", "ml_launch_id").withColumnRenamed("upmid",
                                                                                                 "ml_upmid").withColumnRenamed(
        "received_date", "ml_received_data").withColumnRenamed("is_bot", "ml_bot")

    joined_ml_df = (
        joined_risk_df.join(
            ml_predic_df,
            (F.col("UPMID") == F.col("ml_upmid")) & (F.col("ml_launch_id") == F.col("LAUNCH_ID")),
            how="left"
        )
    )

    # joined_df = joined_df.dropDuplicates(["UPMID", "VALIDATION_RESULT"])
    joined_ml_df = joined_ml_df.dropDuplicates(["LAUNCH_ID", "UPMID", "VALIDATION_RESULT"])

    # 过滤掉human，只留下suspicious,但保留launch的所有upmid
    # susp_df = (
    #   joined_ml_df.join(
    #     susp_df,
    #     (F.col("UPMID")==F.col("susp_upm")),
    #     how="left"
    #     )
    #   )
    # susp_df=susp_df.dropDuplicates(["LAUNCH_ID","UPMID","VALIDATION_RESULT"])

    # 此时完成三表连接：join，筛选需要的字段
    susp_df = joined_ml_df.select(
        "LAUNCH_ID",
        # "operational_tier",
        "UPMID",
        "PRODUCT_MERCH_GROUP",
        # "PRODUCT_COUNTRY",
        "VALIDATION_RESULT",
        "ENTRY_STATUS",
        # 标签来模型表
        "themis_human",
        # 标签来自swatbots_secure.ml_predictions_launch_bots
        "ml_bot",
        # 标签来自airbot_prod.member_risk_score_exclude_demand 需要score_exclude_demand>0才能过滤human_flag
        # "score_exclude_demand",
        # 以下全部来自大的risk表
        "score_w_human_exclude_demand",
        "score_w_human",
        "score",
        "reason",
        "received_date"
    )

    # 统计valid
    # 全局valid 数量
    valid_df = susp_df.filter(F.col("VALIDATION_RESULT") == "VALID")
    valid_nums = valid_df.count()

    # # 创建valid临时表
    # valid_upmid_temp_view=f"launch_{tag}_valid_upmid_detail_temp_view"
    # valid_df.createOrReplaceTempView(valid_upmid_temp_view)

    # winner/valid
    # print(f"winner/valid:{round(winner_nums/valid_nums,4)}")

    if winner is not None:
        # 统计winner
        # winner数量
        winner_df = susp_df.filter(F.col("ENTRY_STATUS") == "WINNER")
        winner_nums = winner_df.count()
        # 创建winner临时表
        winner_upmid_temp_view = f"launch_{tag}_winner_upmid_detail_temp_view"
        winner_df.createOrReplaceTempView(winner_upmid_temp_view)

        # winner risk
        # 无阈值
        winner_suspicous_df = winner_df.filter(F.col("score_w_human_exclude_demand") > 0)
        winner_suspicous_nums = winner_suspicous_df.count()
        # 计算group_reason 排名
        winner_group_reason_df = winner_suspicous_df.groupBy("reason").agg(F.count("*").alias("count")).orderBy(
            F.desc("count"))
        winner_group_reason_view = f"launch_{tag}_winner_non_threshold_group_reason_ranking_temp_view"
        winner_group_reason_df.createOrReplaceTempView(winner_group_reason_view)
        # 计算单个reason排名
        winner_single_reason_df = winner_suspicous_df.withColumn("single_reason", F.explode(F.col("reason"))).groupBy(
            "single_reason").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
        winner_single_reason_view = f"launch_{tag}_winner_non_threshold_single_reason_ranking_temp_view"
        winner_single_reason_df.createOrReplaceTempView(winner_single_reason_view)

        # 有阈值
        winner_risk_df = winner_suspicous_df.filter(F.col("score") >= 35)
        winner_risk_nums = winner_risk_df.count()
        # 计算分组reason
        winner_threshold_35_group_reason_df = winner_risk_df.groupBy("reason").agg(F.count("*").alias("count")).orderBy(
            F.desc("count"))
        winner_threshold_35_group_reason_view = f"launch_{tag}_winner_threshold_35_group_reason_ranking_temp_view"
        winner_threshold_35_group_reason_df.createOrReplaceTempView(winner_threshold_35_group_reason_view)

        # 计算单个reason
        winner_threshold_35_single_reason_df = winner_risk_df.withColumn("single_reason",
                                                                         F.explode(F.col("reason"))).groupBy(
            "single_reason").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
        winner_threshold_35_single_reason_view = f"launch_{tag}_winner_threshold_35_single_reason_ranking_temp_view"
        winner_threshold_35_single_reason_df.createOrReplaceTempView(winner_threshold_35_single_reason_view)

        # print("Winner_detail:")
        print(f"winner数量:{winner_nums}")
        print(f'winner_themis_bot数量:{winner_df.filter(F.col("themis_human") == 0).count()}')
        print(f'winner_ml_bot数量:{winner_df.filter(F.col("ml_bot") == 1).count()}')
        print(f"winner_suspicous_bot数量:{winner_suspicous_nums} (non_threshold)")
        print(f"winner_suspicous_bot/winner: {round(winner_suspicous_nums / winner_nums, 4)} (non_threshold)")
        print(f'winner_risk_bot数量:{winner_risk_nums} (threshold_35)')
        print(f"winner_risk_bot/winner: {round(winner_risk_nums / winner_nums, 4)} (threshold_35)")

        # print(f"----为launch_{tag}_winner_upmid创建详情表(可查询)：")
        # print(f"{winner_upmid_temp_view}")
        # winner无阈值的创建了reason排名表
        print(f"为launch_{tag}_winner创建reason排名表(可查询):")
        print(f"{winner_group_reason_view}  (non_threshold)")
        print(f"{winner_single_reason_view}  (non_threshold)")
        # winner阈值35，创建了reason排名表
        print(f"{winner_threshold_35_group_reason_view} (threshold_35)")
        print(f"{winner_threshold_35_single_reason_view} (threshold_35)")
        print(f"------------------------------------------------")
        print(f"valid数量:{valid_nums}")
        # winner/valid
        print(f"winner/valid:{round(winner_nums / valid_nums, 4)}")
        print(f"END!!! 返回launch_{tag}_winner_upmid_detail_df")
        return winner_df

    else:
        # 统计valid
        # valid 数量
        # valid risk
        # 无阈值
        valid_suspicous_df = valid_df.filter(F.col("score_w_human_exclude_demand") > 0)
        valid_suspicous_nums = valid_suspicous_df.count()
        # 计算group_reason排名
        valid_group_reason_df = valid_suspicous_df.groupBy("reason").agg(F.count("*").alias("count")).orderBy(
            F.desc("count"))
        valid_group_reason_view = f"launch_{tag}_valid_non_threshold_group_reason_ranking_temp_view"
        valid_group_reason_df.createOrReplaceTempView(valid_group_reason_view)
        # 计算单个reason
        valid_single_reason_df = valid_suspicous_df.withColumn("single_reason", F.explode(F.col("reason"))).groupBy(
            "single_reason").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
        valid_single_reason_view = f"launch_{tag}_valid_non_threshold_single_reason_ranking_temp_view"
        valid_single_reason_df.createOrReplaceTempView(valid_single_reason_view)

        # 有阈值
        valid_risk_df = valid_suspicous_df.filter(F.col("score") >= 35)
        valid_risk_nums = valid_risk_df.count()
        # 计算分组reason
        valid_threshold_35_group_reason_df = valid_risk_df.groupBy("reason").agg(F.count("*").alias("count")).orderBy(
            F.desc("count"))
        valid_threshold_35_group_reason_view = f"launch_{tag}_valid_threshold_35_group_reason_ranking_temp_view"
        valid_threshold_35_group_reason_df.createOrReplaceTempView(valid_threshold_35_group_reason_view)

        # 计算单个reason
        valid_threshold_35_single_reason_df = valid_risk_df.withColumn("single_reason",
                                                                       F.explode(F.col("reason"))).groupBy(
            "single_reason").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
        valid_threshold_35_single_reason_view = f"launch_{tag}_valid_threshold_35_single_reason_ranking_temp_view"
        valid_threshold_35_single_reason_df.createOrReplaceTempView(valid_threshold_35_single_reason_view)

        # print("Valid_detail:")
        print(f"valid数量:{valid_nums}")
        print(f'valid_themis_bot数量:{valid_df.filter(F.col("themis_human") == 0).count()}')
        print(f'valid_ml_bot数量:{valid_df.filter(F.col("ml_bot") == 1).count()}')
        print(f"valid_suspicous_bot数量:{valid_suspicous_nums} (non_threshold)")
        print(f"valid_suspicous_bot/valid: {round(valid_suspicous_nums / valid_nums, 4)} (non_threshold)")
        print(f'valid_risk_bot数量:{valid_risk_nums} (threshold_35)')
        print(f"valid_risk_bot/valid: {round(valid_risk_nums / valid_nums, 4)} (threshold_35)")

        # print(f"----为launch_{tag}_valid_upmid创建详情表(可查询)：")
        # print(f"{valid_upmid_temp_view}")
        # valid无阈值的创建了reason排名表
        print(f"为launch_{tag}_valid创建reason排名表(可查询):")
        print(f"{valid_group_reason_view} (non_threshold)")
        print(f"{valid_single_reason_view} (non_threshold)")
        # valid阈值35，创建了reason排名表
        print(f"{valid_threshold_35_group_reason_view} (threshold_35)")
        print(f"{valid_threshold_35_single_reason_view} (threshold_35)")
        print(f"END!!! 返回launch_{tag}_valid_upmid_detail_df")
        return susp_df
