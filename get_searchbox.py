import random
import re
import requests
from lxml import etree
from settings import HEADERS
from store import AmazonStorePro
from parse_product_html import SiteType


sql_insert = ("insert into crawler_wcs_amazon_search_category(site, search_category, search_alias, create_time)"
              "values(%s,%s,%s,now())")

amazon_store = AmazonStorePro()


def get_searchword(url):
    suffix = re.findall(r'www.amazon.(.+)', url)[0]
    print(suffix)
    headers = {'user-agent': random.choice(HEADERS)}
    req = requests.get(url, headers=headers)
    if req.status_code == 200:
        sel = etree.HTML(req.text)
        options = sel.xpath('//select[@id="searchDropdownBox"]/option')
        for op in options:
            key_word = op.xpath('./text()')[0].strip()
            parm = op.xpath('./@value')[0].split('=')[1].strip()
            print(key_word, parm)
            amazon_store.execute_sql(sql_insert, suffix, key_word, parm)


if __name__ == '__main__':
    for mp in SiteType.values():
        print(mp['site'])
        get_searchword(mp['site'])
