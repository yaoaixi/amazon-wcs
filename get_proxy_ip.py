# -*- coding: utf-8 -*-
import json
import time
import requests
import traceback
from requests.exceptions import RequestException
from json.decoder import JSONDecodeError

# 私密代理接口
ProxyUrl = 'http://dps.kuaidaili.com/api/getdps/?orderid=941797185104558&num=1000&format=json&sep=1'
GouIp = 'http://dynamic.goubanjia.com/dynamic/get/b94808e4e950d7b06b5370231ae7304d.html'
JieKou = 'http://47.90.32.89:20000/proxy/kuaidaili'


# 接口取代理ip
def get_kdl_ip():
    flag = True
    while flag:
        try:
            res = requests.get(ProxyUrl, timeout=10)
            if res.status_code == 200:
                try:
                    proxy_list = list(set(json.loads(res.text)['data']['proxy_list']))
                except TypeError:
                    time.sleep(2)
                else:
                    flag = False
                    return proxy_list
        except RequestException:
            print('No ProxyIp')
            time.sleep(2)


def get_gou_ip():
    ip_lst = []
    try:
        res = requests.get(GouIp, timeout=10)
        if res.status_code == 200:
            try:
                res.json()['success']
            except JSONDecodeError:
                proxy_list = res.text.strip().split('\n')
                ip_lst.extend(proxy_list)
    except Exception as err:
        traceback.print_exc()
        print('Gou ProxyIp raise a exc {}'.format(err))
    finally:
        return ip_lst


def get_jiekou():
    ip_lst = []
    try:
        res = requests.get(JieKou, timeout=10)
        if res.status_code == 200:
            ip_lst = json.loads(res.text)
            # try:
            #     res.json()['success']
            # except JSONDecodeError:
            #     proxy_list = res.text.strip().split('\n')
            #     ip_lst.extend(proxy_list)
    except Exception as err:
        print('Gou ProxyIp raise a exc {}'.format(err))
    finally:
        return ip_lst


if __name__ == '__main__':
    pass









