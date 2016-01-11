import logging
import urlparse

import requests
import trollius as asyncio
from trollius import From


logger = logging.getLogger(__name__)


@asyncio.coroutine
def get_es_cluster_stats(agent):
    yield From(agent.run_event.wait())
    logger.debug('starting get_elasticsearch_stats')
    config = agent.config['elasticsearch']
    logger.debug('get_elasticsearch_stats config retrieved')
    db_config = config['database']
    yield From(agent.async_create_database(**db_config))
    logger.debug('getting event loop')
    loop = asyncio.get_event_loop()
    base_url = config['base_url']
    while agent.run_event.is_set():
        logger.debug('in while loop')
        try:
            yield From(asyncio.sleep(config['frequency']))
            points = [{
                'measurement': 'es_cluster_stats',
                'tags': {
                    'hostname': config['host'],
                },
                'fields': {
                }
            }]
            cluster_stats_url = urlparse.urljoin(base_url,
                                                 '_cluster/stats')
            res = yield From(loop.run_in_executor(
                None, requests.get, cluster_stats_url))
            fields = points[0]['fields']
            if res.status_code == 200:
                info = res.json()
                points[0]['tags']['cluster_name'] = info['cluster_name']
                fields['cluster_status'] = info['status']
                fields['cluster_total_indices'] =\
                    info['indices']['count']
                fields['cluster_total_shards'] =\
                    info['indices']['shards']['total']
                fields['cluster_primaries_shards'] =\
                    info['indices']['shards']['primaries']
                fields['cluster_replication_shards'] =\
                    info['indices']['shards']['replication']
                fields['cluster_total_docs'] =\
                    info['indices']['docs']['count']
                fields['cluster_total_deleted_docs'] =\
                    info['indices']['docs']['deleted']
                fields['cluster_store_size_in_bytes'] =\
                    info['indices']['store']['size_in_bytes']
                fields['cluster_store_throttle_time_in_millis'] =\
                    info['indices']['store']['throttle_time_in_millis']
                fields['cluster_fielddata_memory_size_in_bytes'] =\
                    info['indices']['fielddata']['memory_size_in_bytes']
                fields['cluster_fielddata_evictions'] =\
                    info['indices']['fielddata']['evictions']
                fields['cluster_filter_cache_memory_size_in_bytes'] =\
                    info['indices']['filter_cache']['memory_size_in_bytes']
                fields['cluster_filter_cache_evictions'] =\
                    info['indices']['filter_cache']['evictions']
                fields['cluster_id_cache_memory_size_in_bytes'] =\
                    info['indices']['id_cache']['memory_size_in_bytes']
                fields['cluster_completion_size_in_bytes'] =\
                    info['indices']['completion']['size_in_bytes']
                fields['cluster_segments_count'] =\
                    info['indices']['segments']['count']
                fields['cluster_segments_count'] =\
                    info['indices']['segments']['count']
                fields['cluster_percolate_total'] =\
                    info['indices']['percolate']['total']
                fields['cluster_percolate_time_in_millis'] =\
                    info['indices']['percolate']['time_in_millis']
                fields['cluster_percolate_memory_size_in_bytes'] =\
                    info['indices']['percolate']['memory_size_in_bytes']
                fields['cluster_process_cpu_percent'] =\
                    info['nodes']['process']['cpu']['percent']
                fields['cluster_process_open_file_descriptors_min'] =\
                    info['nodes']['process']['open_file_descriptors']['min']
                fields['cluster_process_open_file_descriptors_max'] =\
                    info['nodes']['process']['open_file_descriptors']['max']
                fields['cluster_process_open_file_descriptors_avg'] =\
                    info['nodes']['process']['open_file_descriptors']['avg']
                fields['cluster_jvm_mem_heap_used_in_bytes'] =\
                    info['nodes']['jvm']['mem']['heap_used_in_bytes']
                fields['cluster_jvm_mem_heap_max_in_bytes'] =\
                    info['nodes']['jvm']['mem']['heap_max_in_bytes']
                fields['cluster_jvm_threads'] =\
                    info['nodes']['jvm']['threads']
                fields['cluster_fs_total_in_bytes'] =\
                    info['nodes']['fs']['total_in_bytes']
                fields['cluster_fs_free_in_bytes'] =\
                    info['nodes']['fs']['free_in_bytes']
                fields['cluster_fs_available_in_bytes'] =\
                    info['nodes']['fs']['available_in_bytes']
                fields['cluster_fs_disk_reads'] =\
                    info['nodes']['fs']['disk_reads']
                fields['cluster_fs_disk_writes'] =\
                    info['nodes']['fs']['disk_writes']
                fields['cluster_fs_disk_io_op'] =\
                    info['nodes']['fs']['disk_io_op']
                fields['cluster_fs_disk_read_size_in_bytes'] =\
                    info['nodes']['fs']['disk_read_size_in_bytes']
                fields['cluster_fs_disk_write_size_in_bytes'] =\
                    info['nodes']['fs']['disk_write_size_in_bytes']
                fields['cluster_fs_disk_io_size_in_bytes'] =\
                    info['nodes']['fs']['disk_io_size_in_bytes']
                fields['cluster_fs_disk_queue'] =\
                    info['nodes']['fs']['disk_queue']
                fields['cluster_disk_service_time'] =\
                    info['nodes']['fs']['disk_service_time']
                logger.debug('es data: {}'.format(points))
                yield From(agent.async_push(points, db_config['name']))
            else:
                logger.warning('cannot get nginx stats: status={}'
                               .format(res.status_code))
        except:
            logger.exception('cannot get nginx stats')
    logger.info('get_elasticsearch_stats terminated')
