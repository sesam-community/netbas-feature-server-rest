from flask import Flask, request, Response
import os
from sesamutils import sesam_logger
import requests
import logging
import json
from time import sleep

app = Flask(__name__)
logger = sesam_logger("Steve the logger", app=app)

# Environment variables
required_env_vars = ["BASE_URL", "ENTITIES_PATH", "RESULT_RECORD_COUNT"]
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


def get_paged_entities(path, entities):
        logger.info("Fetching data from paged url: %s", path)
        NEXT_PAGE = True
        headers={"Content-Type":"application/json","Accept":"application/json"}
        #headers={"Content-Type":"application/json;charset=utf-8", "Request-Context":"appId=cid-v1:", "Server":"Kestrel", "Access-Control-Allow-Origin":"*", "Etag":"0c616836", "Transfer-Encoding":"chunked"}
        RESULT_OFFSET = 0
        RESULT_RECORD_COUNT = getattr(config, 'RESULT_RECORD_COUNT', 1000)
        entities_element = getattr(config, 'ENTITIES_PATH', 'features')
        while NEXT_PAGE is not False:
            try: 
                URL = getattr(config, 'BASE_URL') + path + '/query?outFields=*&resultOffset=' + str(RESULT_OFFSET) + '&resultRecordCount=' + str(RESULT_RECORD_COUNT) + '&f=json'
                logger.info("Fetching data from url: %s", URL)
            
                req = requests.get(URL, headers=headers)
                if not req.ok:
                    logger.error("Unexpected response status code: %d with response text %s" % (req.status_code, req.text))
                    raise AssertionError ("Unexpected response status code: %d with response text %s"%(req.status_code, req.text))
                
                logger.info(req.content)
                res = json.loads(req.content.decode('utf-8-sig'))
                entities.extend(res[f'{entities_element}'])
                logger.info(f"extending result as exceed page limit is still {NEXT_PAGE}")

                try:
                    NEXT_PAGE = decoded_data["exceededTransferLimit"]
                except Exception:
                    NEXT_PAGE = False
                
                if NEXT_PAGE is not False:
                    RESULT_OFFSET+=int(RESULT_RECORD_COUNT)
                    logger.info(f"Result offset is now {RESULT_OFFSET}")

            except Exception as e:
                logger.warning(f"Service not working correctly. Failing with error : {e}")

        logger.info('Returning entities')
        return entities

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
    entities = []
    entities = get_paged_entities(path, entities)

    return Response(
        stream_json(entities),
        mimetype='application/json'
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)