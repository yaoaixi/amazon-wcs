import requests


def get_mail_addr(user_id):
    url = 'https://ema-backend.banggood.cn/crawler/email?userId={}'.format(user_id)
    addr = None
    try:
        rst = requests.get(url)
        if rst.status_code == 200:
            if int(rst.json()['status']) == 1:
                addr = rst.json()['data']
    except Exception as err:
        print('get_mail_addr raise err {}'.format(err))
    return addr


if __name__ == '__main__':
    print(get_mail_addr(60))
