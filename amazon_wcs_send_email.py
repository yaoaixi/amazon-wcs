import time
from email.mime.text import MIMEText
from configuration_file import UserEmail
from scan_task import RedisA
from send_email import SendEmail


def send_mail():
    while RedisA.exists_key(UserEmail):
        user_email = RedisA.rc.lpop(UserEmail)
        mail = SendEmail()
        prefix = 'EMA'
        title = '亚马逊自助采集服务'
        # context = '您的采集任务已完成，请登录网页( https://data.banggood.cn/bgbdp/index#/dataApplication/ponelThree )查看采集结果。'
        # msg = MIMEText(context, 'plain', 'utf-8')
        with open('email.html', encoding='utf-8') as f:
            context = f.read()
            msg = MIMEText(context, 'html', 'utf-8')
        mail.send_message(prefix, user_email, title, msg)
    print('wait 30 s')
    time.sleep(30)


if __name__ == '__main__':
    while True:
        send_mail()
