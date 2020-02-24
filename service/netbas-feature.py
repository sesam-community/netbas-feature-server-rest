from flask import Flask, Response
import os
from sesamutils import sesam_logger
import requests
import json

app = Flask(__name__)
logger = sesam_logger("Steve the logger", app=app)

# Environment variables
required_env_vars = ["BASE_URL", "ENTITIES_PATH", "RESULT_RECORD_COUNT"]
optional_env_vars = ["LOG_LEVEL", "PORT", "START", "END"]


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
    next_page = True
    result_offset = 0
    result_record_count = getattr(config, 'RESULT_RECORD_COUNT', 2000)
    start = getattr(config, 'START', 1)
    end = getattr(config, 'END', 1000)
    entities_element = getattr(config, 'ENTITIES_PATH', 'features')
    while next_page is True:
        try: 
            url = getattr(config, 'BASE_URL') + path + '/query?outFields=*&resultOffset=' + str(result_offset) + '&resultRecordCount=' + str(result_record_count) + '&f=json'
            logger.info("Fetching data from url: %s", url)
        
            req = requests.get(url)
            if not req.ok:
                logger.error("Unexpected response status code: %d with response text %s" % (req.status_code, req.text))
                raise AssertionError ("Unexpected response status code: %d with response text %s"%(req.status_code, req.text))
            
            res = json.loads(req.content.decode('utf-8-sig'))
            for entity in res[entities_element]:
                yield entity

            next_page = res["exceededTransferLimit"]
            if next_page is not False:
                logger.info(f"extending result as exceed page limit is still {next_page}")
                result_offset+=int(result_record_count)
                logger.info(f"Result offset is now {result_offset}")
            
            else:
                logger.info(f"currently on last page...")

        except Exception as e:
            logger.warning(f"Service not working correctly. Failing with error : {e}")

    if next_page is False:
        for i in range(int(start),int(end)):
            if next_page == "Done":
                break

            url = getattr(config, 'BASE_URL') + path + '/query?outFields=*&resultOffset=' + str(result_offset) + '&resultRecordCount=' + str(int(result_record_count)-i) + '&f=json'
            req = requests.get(url)

            res = json.loads(req.content.decode('utf-8-sig'))
            next_page = res["exceededTransferLimit"]
            if next_page is not True:
                logger.info(f"still not correct length on last page...")
            if next_page is True:
                for entity in res[entities_element]:
                    yield entity
                logger.info(f"streamed data from last page...")
                next_page = "Done"


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
    entities = get_paged_entities(path)

    return Response(
        stream_json(entities),
        mimetype='application/json'
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)