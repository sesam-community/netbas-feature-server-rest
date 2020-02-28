# netbas-feature-server-rest

For incremental sync towards feature-server layer in front of netbas.

Environment variables that are required are BASE_URL, ENTITIES_PATH and RESULT_RECORD_COUNT


## pitfalls...
- result_record_count of 1000 is the only supported record count in the API a the moment.

# System config
```
{
	"_id":"netbas-feature-server", 
	"type": "system:microservice",
	"docker":{
		"environment": {
			"BASE_URL":"base URL",
			"ENTITIES_PATH": "features",	
			"RESULT_RECORD_COUNT": 1000
	},
	"image":"sesamcommunity/netbas-feature-server-rest",
	"port":5000
	},
	"verify_ssl":true
}
```

# Pipe config
```
{
	"_id": "netbas-object",
	"type":"pipe",
	"source": {
		"type":"json",
		"system":"netbas-feature-server",
		"url": "feature"
	}
}
```
