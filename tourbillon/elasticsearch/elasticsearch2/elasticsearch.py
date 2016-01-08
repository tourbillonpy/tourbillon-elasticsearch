import logging
import urlparse

import requests
import trollius as asyncio
from trollius import From


logger = logging.getLogger(__name__)


@asyncio.coroutine
def get_elasticsearch_stats(agent):
    yield From(agent.run_event.wait())
    logger.debug('starting get_elasticsearch_stats')
    config = agent.config['elasticsearch']
    logger.debug('get_elasticsearch_stats config retrieved')
    db_config = config['database']
    yield From(agent.async_create_database(**db_config))
    logger.debug('getting event loop')
    loop = asyncio.get_event_loop()
    base_url = config['base_url']
    cluster_stats = True if config['cluster_stats'] == 'true' else False
    while agent.run_event.is_set():
        logger.debug('in while loop')
        try:
            yield From(asyncio.sleep(config['frequency']))
            points = [{
                'measurement': 'elasticsearch_stats',
                'tags': {
                    'hostname': config['host']
                },
                'fields': {
                }
            }]
            if cluster_stats:
                cluster_stats_url = urlparse.urljoin(base_url,
                                                     '_cluster/stats')
                res = yield From(loop.run_in_executor(
                    None, requests.get, cluster_stats_url))

                if res.status_code == 200:
                    info = res.json()
                    points[0]['tags']['cluster_name'] = info['cluster_name']
                    points[0]['fields']['cluster_status'] =\
                        info['cluster_status']
                    points[0]['fields']['cluster_total_indices'] =\
                        info['indices']['count']
                    points[0]['fields']['cluster_total_shards'] =\
                        info['indices']['shards']['total']
                    points[0]['fields']['cluster_primaries_shards'] =\
                        info['indices']['shards']['primaries']
                    points[0]['fields']['cluster_replication_shards'] =\
                        info['indices']['shards']['replication']
                    points[0]['fields']['cluster_total_docs'] =\
                        info['docs']['count']
                    points[0]['fields']['cluster_total_deleted_docs'] =\
                        info['docs']['deleted']
                    points[0]['fields']['cluster_store_size_in_bytes'] =\
                        info['store']['size_in_bytes']
                    points[0]['fields']\
                        ['cluster_store_throttle_time_in_millis'] =\
                        info['store']['throttle_time_in_millis']
                    points[0]['fields']\
                        ['cluster_fielddata_memory_size_in_bytes'] =\
                        info['fielddata']['memory_size_in_bytes']
                    points[0]['fields']\
                        ['cluster_fielddata_evictions'] =\
                        info['fielddata']['evictions']
                    points[0]['fields']\
                        ['cluster_filter_cache_memory_size_in_bytes'] =\
                        info['filter_cache']['memory_size_in_bytes']
                    points[0]['fields']\
                        ['cluster_filter_cache_evictions'] =\
                        info['filter_cache']['evictions']
                    points[0]['fields']\
                        ['cluster_id_cache_memory_size_in_bytes'] =\
                        info['id_cache']['memory_size_in_bytes']
                    points[0]['fields']\
                        ['cluster_completion_size_in_bytes'] =\
                        info['completion']['size_in_bytes']
                    points[0]['fields']\
                        ['cluster_segments_count'] =\
                        info['segments']['count']
                    points[0]['fields']\
                        ['cluster_segments_count'] =\
                        info['segments']['count']
                logger.debug('nginx data: {}'.format(data))
                yield From(agent.async_push(data, db_config['name']))
            else:
                logger.warning('cannot get nginx stats: status={}'
                               .format(res.status_code))
        except:
            logger.exception('cannot get nginx stats')
    logger.info('get_elasticsearch_stats terminated')
