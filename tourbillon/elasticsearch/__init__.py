import sys

PY34_PLUS = sys.version_info[0] == 3 and sys.version_info[1] >= 4

if PY34_PLUS:
    from .elasticsearch.elasticsearch import get_elasticsearch_stats
else:
    from .elasticsearch2.elasticsearch import get_elasticsearch_stats
