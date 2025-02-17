
def get_target_list(user_object):
    target_list = []
    for metric, table_list in user_object.metric_table.items():
        for table_name in table_list:
            # 已识别指标且无任何聚合方式，不会执行extract_aggregation，此时user_object.metric_aggregation为空
            targetName_no_aggregation = table_name + '-' + metric
            if metric in user_object.metric_aggregation.keys():
                aggregation_name = user_object.metric_aggregation[metric][table_name]
                targetName = table_name + '-' + metric + aggregation_name
                # 例如 抢救次数 和 抢救次数合计 都是指标 这时要考虑都选还是选一个
                if targetName_no_aggregation in user_object.metric2id.keys():
                    if targetName in user_object.metric2id.keys():
                        user_input = user_object.history[0]["user"]
                        if aggregation_name in user_input:
                            if user_input.count(metric) == 2:
                                targetId = user_object.metric2id[targetName]
                                target_list.append({"targetId": targetId, "targetName": targetName})
                                targetId = user_object.metric2id[targetName_no_aggregation]
                                target_list.append({"targetId": targetId, "targetName": targetName_no_aggregation})
                            else:
                                targetId = user_object.metric2id[targetName]
                                target_list.append({"targetId": targetId, "targetName": targetName})

                        else:
                            targetId = user_object.metric2id[targetName_no_aggregation]
                            target_list.append({"targetId": targetId, "targetName": targetName_no_aggregation})
                    else:
                        targetId = user_object.metric2id[targetName_no_aggregation]
                        target_list.append({"targetId": targetId, "targetName": targetName_no_aggregation})
                else:
                    if targetName in user_object.metric2id.keys():
                        targetId = user_object.metric2id[targetName]
                        target_list.append({"targetId": targetId, "targetName": targetName})

            else:
                if targetName_no_aggregation in user_object.metric2id.keys():
                    targetId = user_object.metric2id[targetName_no_aggregation]
                    target_list.append({"targetId": targetId, "targetName": targetName_no_aggregation})

    return target_list


