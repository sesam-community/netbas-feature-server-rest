from flask import Flask, Response
import os
from sesamutils import sesam_logger
import requests
import json
from requests_ntlm import HttpNtlmAuth

app = Flask(__name__)
logger = sesam_logger("Steve the logger", app=app)

# Environment variables
required_env_vars = ["BASE_URL", "ENTITIES_PATH", "RESULT_RECORD_COUNT", "SYSTEM_USER", "SYSTEM_PASSWORD"]
optional_env_vars = ["LOG_LEVEL", "PORT"]


class AppConfig(object):
    pass


config = AppConfig()

# load variables
missing_env_vars = list()
for env_var in required_env_vars:
    value = os.getenv(env_var)
    if not value:
        missing_env_vars.append(env_var)
    setattr(config, env_var, value)

for env_var in optional_env_vars:
    value = os.getenv(env_var)
    if value:
        setattr(config, env_var, value)


def get_paged_entities(path):
    logger.info("Fetching data from paged url: %s", path)
    result_offset = 0
    result_record_count = getattr(config, 'RESULT_RECORD_COUNT', 1000)
    entities_element = getattr(config, 'ENTITIES_PATH', 'features')
    user = getattr(config, 'SYSTEM_USER')
    password = getattr(config, 'SYSTEM_PASSWORD')
    url_count = getattr(config, 'BASE_URL') + path + '/query?returnCountOnly=True'
    request_for_count = requests.get(url=url_count, auth=HttpNtlmAuth(user, password), verify=False)
    result_for_count = json.loads(request_for_count.content.decode('utf-8-sig'))
    result_count = result_for_count["count"]
    logger.info(f"Fetching count from url with value of : {result_count}")
    count = 0
    small_data_count = 0
    result_count = result_count-1
    while result_count > result_offset and count == 0:
        expected_count = result_offset + int(result_record_count)

        if expected_count > result_count and small_data_count == 0:
            result_record_count = result_count+1
            expected_count = result_count
        
        url = getattr(config, 'BASE_URL') + path + '/query?outFields=*&resultOffset=' + str(result_offset) + '&resultRecordCount=' + str(result_record_count) + '&f=json'
        logger.info("Fetching data from url: %s", url)
        
        req = requests.get(url=url, auth=HttpNtlmAuth(user, password), verify=False)
        if not req.ok:
            logger.error("Unexpected response status code: %d with response text %s" % (req.status_code, req.text))
            raise AssertionError ("Unexpected response status code: %d with response text %s"%(req.status_code, req.text))
        
        res = json.loads(req.content.decode('utf-8-sig'))

        for entity in res[entities_element]:
            try:
                yield entity
            except Exception as e:
                logger.error(f"Failing to stream entity with error: {e}")
        
        if result_count == expected_count and count == 0:
            logger.info("all data fetched")
            count = count + 1
        
        if expected_count < result_count:
            small_data_count = small_data_count + 1
            logger.info(f"extending result as exceed page limit is still True")
            result_offset += int(result_record_count)
            expected_count = result_offset + int(result_record_count)
            if expected_count > result_count:
                result_record_count = result_count-result_offset
            else:
                logger.info(f"Result offset is now {result_offset}")


def generator(path):
    yield from get_paged_entities(path)

def stream_json(clean):
    first = True
    yield '['
    for i, row in enumerate(clean):
        if not first:
            yield ','
        else:
            first = False
        yield json.dumps(row)
    yield ']'


@app.route("/<path:path>", methods=["GET"])
def get(path):
    entities = generator(path)

    return Response(
        stream_json(entities),
        mimetype='application/json'
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)