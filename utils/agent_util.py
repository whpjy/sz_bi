
def get_relevantIndicator_targetId(targetDefine_dict, relevantIndicatorName_list, all_metric_data):

    relevantIndicator = []
    if len(relevantIndicatorName_list) == 0:
        return relevantIndicator

    # 只要前10个
    relevantIndicatorName_list = relevantIndicatorName_list[:10]

    for name in relevantIndicatorName_list:
        targetJson = all_metric_data["targetJson"]
        for info in targetJson:
            targetName = info["targetName"]
            targetId = info["targetId"]
            if name in targetName:
                targetDefine = ""
                if targetName in targetDefine_dict.keys():
                    targetDefine = targetDefine_dict[targetName]

                indicator_obj = {"targetId": targetId, "targetName": targetName, "targetDefine": targetDefine}
                if indicator_obj not in relevantIndicator:
                    relevantIndicator.append(indicator_obj)

    print(relevantIndicator)
    return relevantIndicator
