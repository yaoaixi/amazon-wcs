import datetime
import time
import queue
from configuration_file import (
    StartUrls, OneTask, DetailTag, ListTag, KeyWordTag, BestSellersTag, NewReleasesTag, RedisSpace, WaitSec,
    Temp, ProxyIpFailTimes)
from store import RedisCluster, AmazonStorePro
from post_data import PostData
from get_proxy_ip import get_kdl_ip, get_gou_ip, get_jiekou

RedisA = RedisCluster()
Que = queue.Queue()


def update_proxy_ip(que):
    try:
        for n in range(que.qsize()):
            que.get_nowait()
    except queue.Empty:
        pass
    ip_lst = get_gou_ip()
    if not ip_lst:
        ip_lst = get_jiekou()
    for ip_proxy in ip_lst:   # 切换ip运营商
        que.put({'ip': ip_proxy, 'num': ProxyIpFailTimes})
    print('update proxy ip: {!r}'.format(que.qsize()))


def change_status(status, code, task_id):
    if Temp:
        pd = PostData()
        pd.update(wtcPlatform="amazon", wtcStatus=status, wtcId=task_id)
    else:
        store = AmazonStorePro()
        sql_update_status = ("update crawler_wcs_task_center set wtc_status=%s, wtc_error_info=%s, "
                             "wtc_crawl_time=now() where wtc_id=%s")
        store.execute_sql(sql_update_status, status, code, task_id)
        store.close()


def scan_database():
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    flag = False
    store = AmazonStorePro()
    pd = PostData()
    if Temp:
        rows_0 = pd.select(wtcPlatform="amazon", wtcStatus=0, limit=1)
    else:
        sql_select = (
            "select wtc_id, wtc_user_id, wtc_task_type, wtc_task_frequency, wtc_task_period, wtc_task_info,wtc_task_category," 
            "wtc_task_product_id, wtc_task_site from crawler_wcs_task_center where wtc_status=%s and wtc_platform=%s"
            "and wtc_is_delete=%s limit 1")
        rows_0 = store.execute_sql(sql_select, 0, 'amazon', 0)

    if rows_0:
        row_dct = rows_0[0]
        print(row_dct)
        if Temp:
            task_id = row_dct['wtcId']
            try:
                user_id = row_dct['wtcUserId']
                task_type = row_dct['wtcTaskType']
                task_frequency = row_dct['wtcTaskFrequency']
                task_period = row_dct['wtcTaskPeriod']
                task_info = row_dct['wtcTaskInfo']
                task_category = row_dct['wtcTaskCategory']
                task_asin = row_dct['wtcTaskProductId']
                task_site = row_dct['wtcTaskSite']
            except KeyError:
                print("KeyError")
                change_status(-1, '', task_id)
                return
        else:
            task_id = row_dct['wtc_id']
            user_id = row_dct['wtc_user_id']
            task_type = row_dct['wtc_task_type']
            task_frequency = row_dct['wtc_task_frequency']
            task_period = row_dct['wtc_task_period']
            task_info = row_dct['wtc_task_info']
            task_category = row_dct['wtc_task_category']
            task_asin = row_dct['wtc_task_product_id']
            task_site = row_dct['wtc_task_site']

        # 详情页
        task_num = 1
        if task_type == DetailTag:
            if not (task_asin and task_site):
                change_status(-1, '', task_id)   # 前端已控制
                return
            task_site = task_site.strip()
            task_asin_set = {item.strip() for item in task_asin.strip().replace('，', ',').split(',') if item}
            task_num = len(task_asin_set)
            for task_asin in task_asin_set:
                print(task_asin)
                page_url = 'https://www.amazon.{}/dp/{}'.format(task_site, task_asin)
                mp = {'entry': task_type, 'page_url': page_url, 'task_id': task_id}
                RedisA.rc.rpush(StartUrls, mp)

        # 关键词
        elif task_type == KeyWordTag:
            if not (task_site and task_info) or (task_info.strip().startswith('http')):
                change_status(-1, '601', task_id)   # 601
                return
            task_site = task_site.strip()
            task_info = task_info.strip()
            keyword = '+'.join(task_info.split())
            task_category = task_category.strip()
            sql_alias = ("select search_alias from crawler_wcs_amazon_search_category where site=%s and "
                         "search_category=%s")
            search_alias = store.execute_sql(sql_alias, task_site, task_category)
            if search_alias:
                search_alias = search_alias[0]['search_alias']
                page_url = 'https://www.amazon.{}/s/?url=search-alias%3D{}&field-keywords={}'.format(task_site,
                                                                                                     search_alias,
                                                                                                     keyword)
            else:
                change_status(-1, '602', task_id)
                return
            mp = {'entry': task_type, 'page_url': page_url, 'task_id': task_id, 'search_box': task_category.strip()}
            RedisA.rc.rpush(StartUrls, mp)

        # 列表，热销和新品
        elif task_type in (ListTag, BestSellersTag, NewReleasesTag):   # 2,4,5
            if not task_info or (not task_info.strip().startswith('http')):
                change_status(-1, '', task_id)   # 前端已控制
                return
            if "keywords" in task_info:   # 603
                change_status(-1, '603', task_id)
                return
            if task_type == ListTag and (
                    'Best-Sellers' in task_info or 'bestsellers' in task_info or 'best-sellers' in task_info or 'new-releases' in task_info):
                change_status(-1, '603', task_id)
                return
            page_url = task_info.strip()
            mp = {'entry': task_type, 'page_url': page_url, 'task_id': task_id, 'task_category': task_category}
            RedisA.rc.rpush(StartUrls, mp)

        else:
            change_status(-1, '', task_id)   # 前端已控制
            return

        # 单次采集
        if task_frequency == task_period:
            hash_mp = {'is_track': 0, 'task_id': task_id, 'user_id': user_id, 'task_num': task_num}
            RedisA.set_hash(OneTask, hash_mp)
        # 循环采集首次
        elif task_type in (BestSellersTag, NewReleasesTag):   # and task_period > task_frequency
            RedisA.set_hash(OneTask, {'is_track': 1, 'task_id': task_id, 'user_id': user_id})
            key = RedisSpace + str(task_id)
            now_time = time.strftime("%Y-%m-%d %H:%M:%S")
            RedisA.set_hash(key, {'start_track_time': now_time, 'last_track_time': now_time})
        else:
            change_status(-1, '', task_id)   # 前端已控制
            return
        change_status(1, '', task_id)
        update_proxy_ip(Que)

    else:
        # 循环采集
        if Temp:
            rows_1 = pd.select(wtcPlatform="amazon", wtcStatus=1, limit=1000)
        else:
            sql_select_track = (
                "select wtc_id, wtc_task_type, wtc_task_frequency, wtc_task_period, wtc_task_info,wtc_task_category,"
                "wtc_task_product_id, wtc_task_site from crawler_wcs_task_center where wtc_status=%s and "
                "wtc_platform=%s and wtc_is_delete=%s")
            rows_1 = store.execute_sql(sql_select_track, 1, 'amazon', 0)

        for row_1 in rows_1:
            row_dct = row_1

            if Temp:
                task_id = row_dct['wtcId']
                task_type = row_dct['wtcTaskType']
                task_frequency = row_dct['wtcTaskFrequency']
                task_period = row_dct['wtcTaskPeriod']
                task_info = row_dct['wtcTaskInfo']
                task_category = row_dct['wtcTaskCategory']
            else:
                task_id = row_dct['wtc_id']
                task_type = row_dct['wtc_task_type']
                task_frequency = row_dct['wtc_task_frequency']
                task_period = row_dct['wtc_task_period']
                task_info = row_dct['wtc_task_info']
                task_category = row_dct['wtc_task_category']

            key = RedisSpace + str(task_id)
            if RedisA.exists_key(key):
                start_track_time = RedisA.get_hash_field(key, 'start_track_time')
                last_track_time = RedisA.get_hash_field(key, 'last_track_time')
                if isinstance(start_track_time, bytes):
                    start_track_time = start_track_time.decode('utf-8')
                    last_track_time = last_track_time.decode('utf-8')
                start_track_time_dt = datetime.datetime.strptime(start_track_time, "%Y-%m-%d %H:%M:%S")
                last_track_time_dt = datetime.datetime.strptime(last_track_time, "%Y-%m-%d %H:%M:%S")
                end_track_time = start_track_time_dt + datetime.timedelta(days=task_period)
                next_track_time = last_track_time_dt + datetime.timedelta(days=task_frequency)
                now_time = datetime.datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
                if next_track_time > end_track_time:
                    change_status(2, '', task_id)
                    RedisA.delete_key(key)
                if now_time > next_track_time:
                    page_url = task_info.strip()
                    mp = {'entry': task_type, 'page_url': page_url, 'task_id': task_id}
                    if task_category:
                        mp['task_category'] = task_category.strip()
                    RedisA.set_hash(key, {'last_track_time': now_time})
                    change_status(1, '', task_id)
                    RedisA.rc.rpush(StartUrls, mp)
                    print('track: %s' % task_id)
                    update_proxy_ip(Que)
                    break
                print('not track time: %s' % task_id)
        else:
            flag = True
    store.close()
    if flag:
        print('no task, waiting for {} sec.'.format(WaitSec))
        time.sleep(WaitSec)


if __name__ == '__main__':
    pass






