# -*- coding: utf-8 -*-
import random
import time
import asyncio
from threading import Thread
import aiohttp
from aiohttp.client_exceptions import ClientError
from configuration_file import ConcurrentNum, TimeOut, StartUrls, ReqUrls, CrawlUrls, OneTask, RemainNum, UserEmail, TopRank
from store import RedisCluster
from settings import HEADERS
from parse_html_func import exist_captcha, choose_parse
from scan_task import scan_database, change_status, Que, update_proxy_ip
from get_user_mail import get_mail_addr

RedisA = RedisCluster()


def start_loop(loop):
    global flag
    try:
        asyncio.set_event_loop(loop)
        loop.run_forever()
    except:
        print('exc')
        flag = False


async def req_http(mp):
    mapping = eval(mp)
    headers = {'User-Agent': random.choice(HEADERS)}
    proxy_ip = Que.get()
    ip = proxy_ip['ip']
    proxy = 'http://{}'.format(ip)
    url = mapping['page_url']
    category_entry = mapping.get('category_entry', None)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, proxy=proxy, timeout=TimeOut) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    if exist_captcha(html):
                        print('captcha')
                        proxy_ip['num'] -= 1
                        RedisA.add_set(ReqUrls, mp)
                    else:
                        print(url)
                        choose_parse(html, mp, RedisA)
                elif resp.status == 404:
                    print('404')
                    RedisA.remove_member(CrawlUrls, mp)
                    # collect_error(mp, RedisA, error='404')
                    if not category_entry:
                        if RedisA.exists_key(OneTask):
                            task_num = RedisA.get_hash_field(OneTask, 'task_num')
                            task_num = int(task_num) - 1
                            print('task_num', task_num)
                            if task_num == 0:
                                task_id = RedisA.get_hash_field(OneTask, 'task_id')
                                if isinstance(task_id, bytes):
                                    task_id = task_id.decode('utf-8')
                                change_status(-1, '404', int(task_id))
                                RedisA.delete_key(OneTask)
                            else:
                                RedisA.set_hash(OneTask, {'task_num': task_num})
                else:
                    proxy_ip['num'] -= 1
                    print(resp.status)
                    RedisA.add_set(ReqUrls, mp)
    except ClientError:
        print('ClientError')
        proxy_ip['num'] -= 1
        RedisA.add_set(ReqUrls, mp)
    except asyncio.TimeoutError:
        print('Timeout')
        proxy_ip['num'] -= 1
        RedisA.add_set(ReqUrls, mp)
    except Exception as exp:
        proxy_ip['num'] -= 1
        print('Raise Exception: {!r}'.format(exp))
        RedisA.add_set(ReqUrls, mp)
    finally:
        if proxy_ip['num'] > 0:
            Que.put(proxy_ip)


if __name__ == '__main__':

    new_loop = asyncio.new_event_loop()
    thread = Thread(target=start_loop, args=(new_loop,))
    thread.setDaemon(True)
    thread.start()

    members = RedisA.get_all_members(CrawlUrls) | RedisA.get_all_members(ReqUrls)
    for member in members:
        RedisA.rc.lpush(StartUrls, member)
    RedisA.delete_key(CrawlUrls)
    RedisA.delete_key(ReqUrls)

    flag = True
    try:
        while flag:
            if Que.qsize() < RemainNum:
                update_proxy_ip(Que)

            if RedisA.count_members(CrawlUrls) < ConcurrentNum:
                item = RedisA.rc.blpop(StartUrls, timeout=1)
                if item:
                    item = item[1]
                    if isinstance(item, bytes):
                        item = item.decode('utf-8')
                    RedisA.add_set(ReqUrls, item)

            item = RedisA.pop_member(ReqUrls)
            if item:
                if isinstance(item, bytes):
                    item = item.decode('utf-8')
                RedisA.add_set(CrawlUrls, item)
                asyncio.run_coroutine_threadsafe(req_http(item), new_loop)
            # 队列都为空，采集完成
            if not RedisA.exists_key(CrawlUrls) and not RedisA.exists_key(ReqUrls) and not RedisA.exists_key(StartUrls):
                RedisA.delete_key(TopRank)
                if RedisA.exists_key(OneTask):
                    is_track = RedisA.get_hash_field(OneTask, 'is_track')
                    task_id = RedisA.get_hash_field(OneTask, 'task_id')
                    user_id = RedisA.get_hash_field(OneTask, 'user_id')
                    if isinstance(is_track, bytes):
                        is_track = is_track.decode('utf-8')
                        task_id = task_id.decode('utf-8')
                        user_id = user_id.decode('utf-8')
                    if int(is_track) == 0:
                        time.sleep(20)
                        change_status(2, '', int(task_id))
                    # 发送邮件
                    mail_addr = get_mail_addr(user_id)
                    if mail_addr:
                        print(mail_addr)
                        RedisA.rc.rpush(UserEmail, mail_addr)
                    RedisA.delete_key(OneTask)
                scan_database()
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        new_loop.stop()
