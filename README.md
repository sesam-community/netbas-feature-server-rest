# netbas-feature-server-rest

For incremental sync towards feature-server layer in front of netbas.

Environment variables that are required are BASE_URL, NEXT_PAGE, ENTITIES_PATH and RESULT_RECORD_COUNT 

# System config
```
{
	"_id":"netbas-feature-server", 
	"type": "system:microservice",
	"docker":{
		"environment": {
			"BASE_URL":"base URL",
			"ENTITIES_PATH": "features",
			"NEXT_PAGE":"exceededReturnLimit",
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
