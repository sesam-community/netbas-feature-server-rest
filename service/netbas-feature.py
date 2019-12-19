from flask import Flask, request, Response
import os
import requests
import logging
import json
import cherrypy
from time import sleep

app = Flask(__name__)

# Environment variables
required_env_vars = ["BASE_URL", "ENTITIES_PATH", "NEXT_PAGE", "RESULT_RECORD_COUNT"]
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

# Set up logging
format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logger = logging.getLogger('netbas-feature-server')
stdout_handler = logging.StreamHandler()
stdout_handler.setFormatter(logging.Formatter(format_string))
logger.addHandler(stdout_handler)

loglevel = getattr(config, "LOG_LEVEL", "INFO")
level = logging.getLevelName(loglevel.upper())
if not isinstance(level, int):
    logger.warning("Unsupported log level defined. Using default level 'INFO'")
    level = logging.INFO
logger.setLevel(level)

class DataAccess:

#main get function, uses the documentation for getting all data fields that are relevant and not static. path is input from pipe.
    def __get_all_paged_entities(self, path, args):
        logger.info("Fetching data from paged url: %s", path)
        NEXT_PAGE = True
        page_counter = 1
        headers={"Content-Type":"application/json","Accept":"application/json"}
        RESULT_OFFSET = 0
        RESULT_RECORD_COUNT = getattr(config, 'RESULT_RECORD_COUNT', 1000)
        while NEXT_PAGE is not False:

            URL = getattr(config, 'BASE_URL') + path + '/query?outFields=*&resultOffset=' + str(RESULT_OFFSET) + '&resultRecordCount=' + str(RESULT_RECORD_COUNT) + '&f=json'
            if os.environ.get('sleep') is not None:
                logger.info("sleeping for %s milliseconds", os.environ.get('sleep'))
                sleep(float(os.environ.get('sleep')))

            logger.info("Fetching data from url: %s", URL)
            req = requests.get(URL, headers=headers)

            if not req.ok:
                logger.error("Unexpected response status code: %d with response text %s" % (req.status_code, req.text))
                raise AssertionError ("Unexpected response status code: %d with response text %s"%(req.status_code, req.text))
            res = json.loads(req.content.decode('utf-8-sig'))
            logger.info(res)
            NEXT_PAGE = res.get(getattr(config, "NEXT_PAGE"))
            try:
                spatial_entity = spatial_ref(headers, path, RESULT_OFFSET, RESULT_RECORD_COUNT)
            except Exception as e:
                logger.error(f"Could not get spatial reference. Exiting with error : {e}")
            
            for entity in res.get(getattr(config, "ENTITIES_PATH", "features")):

                yield(entity)
                yield ','
                yield(spatial_entity)

            if NEXT_PAGE is not False:
                RESULT_OFFSET+=int(RESULT_RECORD_COUNT)

            else:
                NEXT_PAGE= False
        logger.info('Returning entities from %i pages', page_counter)

    def get_paged_entities(self,path, args):
        print("getting all paged")
        return self.__get_all_paged_entities(path, args)

data_access_layer = DataAccess()

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

def spatial_ref(headers, path, RESULT_OFFSET, RESULT_RECORD_COUNT):
    response = requests.get(target_url, headers=headers)
    response_json = json.loads(response.text)
    spatial_reference = response_json['spatialReference']
    return spatial_reference
          

@app.route("/<path:path>", methods=["GET"])
def get(path):

    if request.method == "GET":
        path = path

    entities = data_access_layer.get_paged_entities(path, args=request.args)

    return Response(
        stream_json(entities),
        mimetype='application/json'
    )


if __name__ == '__main__':
    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.socket_port': int(getattr(config, "PORT", 5000)),
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()