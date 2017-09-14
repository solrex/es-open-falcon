# es-open-falcon
Elasticsearch Monitor Script for Open Falcon

```
$ crontab -l
*/1 * * * * cd /path/to/es-open-falcon && python -u ./bin/es-falcon.py >> /path/to/log/es-open-falcon.log 2>&1
```
