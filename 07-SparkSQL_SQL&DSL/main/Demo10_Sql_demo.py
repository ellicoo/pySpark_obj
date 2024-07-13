# %sql
# -- 有模型时：themis_predictions的is_human 和 probability
# -- 一个launch的所有winner里面的human的比例
# -- 算fairness_score
# with launch as
# (
#   select upmid
#   from launch_secure.launch_entries
#   where LAUNCH_ID='609e997e-57d1-3373-b111-750b04d41a76' and ENTRY_STATUS='WINNER' and received_date= '2024-01-11'
# ),
# pred as
# (
#   select * from swatbots_secure.heuristics_predictions where launch_date = '2024-01-11' and merch_group='KR'
# ),
# result as (
#   select pred.* from launch inner join pred where launch.UPMID = pred.upmid
# ),
# c as
# (
#   select sum(heuristics_is_human) as human_count, count(upmid) as total from result
# )
# select human_count, total ,human_count/total*100 as fairness from c
#
# -- 提升fairness_score。human_count是根据规则表算的，不会变。要提升fairness_score，只能减少total
# -- 加入risk score以后？可能增加bot数量，则可以剪掉bot。增加human占比？
# -- 本活动中，总的人数是8471，human是7935 fairness即human占比93%。虽然fairness已经很高。但是如果剪掉bot。那么total就变少了，
# -- 从而也能再提高fairness分数。
