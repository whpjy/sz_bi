from utils.prompt import prompt_extract_time_relation
from utils.request_util import send_llm


def extract_relationship(user_object):
    real_input = user_object.history[0]["user"]

    prompt = prompt_extract_time_relation.replace("{user_input}", real_input)

    extract_json_result = send_llm(prompt)

    if "分别" in extract_json_result:
        timeRelationship = ["or"]
        print("timeRelationship: or")
    else:
        timeRelationship = ["and"]
        print("timeRelationship: and")

    return timeRelationship
