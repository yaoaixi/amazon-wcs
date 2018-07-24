# -*- coding: utf-8 -*-
import re
import json
import time
import uuid
from lxml import etree
from configuration_file import (
    COUNT, StartUrls, CrawlUrls, ErrorUrls, ListTag, KeyWordTag, BestSellersTag, NewReleasesTag, RedisSpace, DetailTag,
    OneTask, MysqlDataList, RelevanceTable, SkuTable, TrackTable, TopRank)
from scan_task import change_status
from parse_product_html import SiteType, ParseProduct


def push_data_into_redis(rds, data_mp):
    data_json = json.dumps(data_mp)
    rds.rc.lpush(MysqlDataList, data_json)


def try_again(max_num):
    def decorator(func):
        def wrapper(mp, rds, **kwargs):
            mapping = eval(mp)
            try_num = int(mapping.get('try_num', 0))
            try_num += 1
            print("try_num: ", try_num)
            if try_num >= max_num:
                func(mp, rds, **kwargs)
            else:
                rds.remove_member(CrawlUrls, mp)
                mapping['try_num'] = try_num
                rds.rc.lpush(StartUrls, mapping)
        return wrapper
    return decorator


@try_again(3)
def collect_error(mp, rds, **kwargs):
    mapping = eval(mp)
    mapping["time"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    mapping.update(kwargs)
    rds.rc.lpush(ErrorUrls, mapping)
    rds.remove_member(CrawlUrls, mp)
    if not mapping.get('category_url', None):
        if rds.exists_key(OneTask):
            task_id = rds.get_hash_field(OneTask, 'task_id')
            if isinstance(task_id, bytes):
                task_id = task_id.decode('utf-8')
            change_status(-1, '603', int(task_id))
            rds.delete_key(OneTask)


def exist_captcha(html):
    sel = etree.HTML(html)
    captcha = sel.xpath('//input[@id="captchacharacters"]')
    if captcha:
        return True
    return False


def choose_parse(html, mp, rds):
    mapping = eval(mp)
    entry = mapping['entry']
    sel = etree.HTML(html)
    if entry in (ListTag, KeyWordTag):

        if entry == KeyWordTag:
            redirect = sel.xpath('//div[@id="apsRedirectLink"]')
            if redirect:
                collect_error(mp, rds, error='keyword_redirect')
                return

        items = sel.xpath('//ul[starts-with(@class, "s-result")]/li[@data-asin]')
        if items:
            parse_list(html, mp, rds)
        else:
            collect_error(mp, rds, error='no_list_items')   # 603
    elif entry in (BestSellersTag, NewReleasesTag):
        items_1 = sel.xpath('//div[starts-with(@class, "zg_itemImmersion")]')
        items_2 = sel.xpath('//div[starts-with(@class, "zg_itemRow")]')  # 日站
        items_3 = sel.xpath('//ol[starts-with(@id, "zg-ordered-list")]/li')
        new = mapping.get('new', None)
        print(new)
        if not new:
            if items_1 or items_2:
                parse_top(html, mp, rds)
            elif items_3:
                parse_top_new(html, mp, rds)
            else:
                collect_error(mp, rds, error='no_top_items')   # 603
        else:
            if new == 'n' and (items_1 or items_2):
                parse_top(html, mp, rds)
            elif new == 'y' and items_3:
                parse_top_new(html, mp, rds)
            else:
                collect_error(mp, rds, error='no_top_items')   # 603
    else:
        parse_product(html, mp, rds)


# 解析top1000页
def parse_list(html, mp, rds):
    mapping = eval(mp)
    page_url = mapping['page_url']
    entry = mapping['entry']
    task_id = mapping['task_id']
    search_box = mapping.get('search_box', None)
    category_url = mapping.get('category_url', None)
    if not category_url:
        category_url = page_url
        mapping['category_url'] = page_url
    amount = mapping.get('amount', COUNT)

    # 确定站点
    suffix = re.findall(r'www.amazon.(.*?)/', page_url)[0]
    domain = SiteType[suffix]
    sign = domain['sign']
    site = domain['site']
    currency = domain['currency']
    sel = etree.HTML(html)

    result_count_xp = sel.xpath('//*[@id="s-result-count"]/text()')
    if result_count_xp:
        result_count = re.findall(r'of (.+) results', result_count_xp[0])
        if result_count:
            result_count = result_count[0].replace(',', '')
        else:
            result_count = ''
    else:
        result_count = ''

    if entry == 2:
        category = '>'.join(sel.xpath('//*[@id="s-result-count"]/span/*/text()'))
        task_info = category
    else:
        keyword = re.findall(r'field-keywords=(.+$)', category_url)[0]
        task_info = ' '.join(keyword.split('+'))
    running_category_key = '{}{}'.format(RedisSpace, category_url)
    products_lst = sel.xpath('//ul[starts-with(@class, "s-result")]/li[@data-asin]')
    for pl in products_lst:
        count = rds.count_members(running_category_key)
        if COUNT and count >= amount:
            rds.delete_key(running_category_key)
            break
        asin = pl.xpath('./@data-asin')
        if asin:
            asin = asin[0].strip()
            if rds.is_member(running_category_key, asin):
                continue

            product_url = 'https://www.amazon.{}/dp/{}'.format(suffix, asin)
            rds.add_set(running_category_key, asin)
            rank = rds.count_members(running_category_key)
            _uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, asin + suffix)).replace('-', '')
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            relevance_mp = {"wtr_sku_uuid": _uuid, "wtr_sku_rank": rank, "wtr_sku_url": product_url,
                            "wtr_task_id": task_id, "wtr_task_type": entry, "wtr_task_info": task_info,
                            "wtr_platform": "amazon", "wtr_crawl_time": crawl_time, "wtr_create_time": create_time}
            data_mp = {"table": RelevanceTable, "data": relevance_mp}
            push_data_into_redis(rds, data_mp)
            # data_json = json.dumps(data_mp)
            # rds.rc.lpush(MysqlDataList, data_json)
            try:
                original_price = pl.xpath('.//span[contains(@aria-label, "Suggested Retail Price")]/text()')
                if original_price:
                    original_price = ''.join(original_price[0]).replace('from', '').replace(sign, '').replace(currency, '').replace(' ', '').replace('\xa0', '')
                    if currency == 'EUR':
                        original_price = original_price.replace('.', '').replace(',', '.')
                    else:
                        original_price = original_price.replace(',', '')
                else:
                    original_price = 0
                price_1 = pl.xpath('.//span[contains(@class, "a-size-small s-padding-right-micro")]/text()')
                price_2 = pl.xpath('.//span[contains(@class, "sx-price sx-price-large")]/../@aria-label')
                price_3 = pl.xpath(
                    './/span[contains(@class, "a-size-base a-color-price s-price a-text-bold")]/text()')
                price_4 = pl.xpath('.//a[@class="a-link-normal a-text-normal"]/span[@class="a-offscreen"]/text()')
                price_5 = pl.xpath('.//span[@class="sx-price sx-price-large"]')
                if len(price_1) > 0:
                    price = price_1
                elif len(price_2) > 0:
                    price = price_2[0]
                elif len(price_3) > 0:
                    price = price_3
                elif len(price_4) > 0:
                    price = price_4[0]
                elif len(price_5) > 0:
                    price_whole = price_5[0].xpath('./span[@class="sx-price-whole"]/text()')
                    price_fractional = price_5[0].xpath('./sup[@class="sx-price-fractional"]/text()')
                    price = '{}.{}'.format(price_whole[0], price_fractional[0])
                else:
                    price = 0
                max_price = 0
                if price != 0:
                    price = ''.join(price).replace('from', '').replace(sign, '').replace(currency, '')
                    if currency == 'EUR':
                        price = price.replace('.', '').replace(',', '.')
                    else:
                        price = price.replace(',', '')
                    if '-' in price:
                        price, max_price = [p.strip() for p in price.split('-')]

                original_price = float(original_price)
                price = float(price)
                max_price = float(max_price)
            except:
                original_price = 0
                price = 0
                max_price = 0
            new_mp = {'page_url': product_url, 'entry': 1, 'rank': rank, 'uuid': _uuid,
                      'price': price, 'max_price': max_price, 'original_price': original_price,
                      'category_url': category_url, 'category_entry': entry, 'category_info': task_info,
                      'result_count': result_count, 'task_id': task_id}
            if search_box:
                new_mp["search_box"] = search_box
            rds.rc.lpush(StartUrls, new_mp)
    else:
        next_page = sel.xpath('//a[@id="pagnNextLink"]/@href')
        if next_page:
            next_page_url = site + next_page[0]
            mapping['page_url'] = next_page_url
            rds.rc.lpush(StartUrls, mapping)
        else:
            rds.delete_key(running_category_key)
    rds.remove_member(CrawlUrls, mp)


# 解析top100页
def parse_top(html, mp, rds):
    mapping = eval(mp)
    page_url = mapping['page_url']
    entry = mapping['entry']
    task_id = mapping['task_id']
    category_url = mapping.get('category_url', None)
    task_category = mapping.get('task_category', None)
    if not category_url:
        category_url = page_url
        mapping['category_url'] = page_url

    suffix = re.findall(r'www.amazon.(.*?)/', page_url)[0]
    domain = SiteType[suffix]
    sign = domain['sign']
    currency = domain['currency']
    sel = etree.HTML(html)

    category = sel.xpath('//h1[@id="zg_listTitle"]/span/text()')
    if category:
        category = category[0].strip()
    else:
        category = ''
    if task_category:
        category = task_category
    products_lst_1 = sel.xpath('//div[starts-with(@class, "zg_itemImmersion")]')  # 美英法站
    products_lst_2 = sel.xpath('//div[starts-with(@class, "zg_itemRow")]')  # 日站
    products_lst = products_lst_1 if products_lst_1 else products_lst_2
    for pl in products_lst:
        # asin
        asin = pl.xpath('.//div[@data-p13n-asin-metadata]/@data-p13n-asin-metadata')
        if asin:
            asin = eval(asin[0])['asin']
            rank = pl.xpath('.//span[@class="zg_rankNumber"]/text()')
            if rank:
                rank = rank[0].strip().replace('.', '')
            else:
                rank = ''
            try:
                rank = int(rank)
            except ValueError:
                print('rank ValueError')
                continue
            # 处理重复rank和超100排名
            if rds.is_member(TopRank, rank) or rank > 100:
                continue
            rds.add_set(TopRank, rank)

            # 插入关联表
            product_url = 'https://www.amazon.{}/dp/{}'.format(suffix, asin)
            _uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, asin + suffix)).replace('-', '')
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            relevance_mp = {"wtr_sku_uuid": _uuid, "wtr_sku_rank": rank, "wtr_sku_url": product_url,
                            "wtr_task_id": task_id, "wtr_task_type": entry, "wtr_task_info": category,
                            "wtr_platform": 'amazon', "wtr_crawl_time": crawl_time, "wtr_create_time": create_time}
            data_mp = {"table": RelevanceTable, "data": relevance_mp}
            push_data_into_redis(rds, data_mp)

            try:
                price_1 = pl.xpath('.//span[starts-with(@class, "a-size-base a-color-price")]/span/text()')
                if price_1:
                    _price = ''.join(price_1).replace(sign, '').replace(currency, '').replace(' ', '').replace('\xa0', '')
                    if currency == 'EUR':
                        _price = _price.replace('.', '').replace(',', '.')
                    else:
                        _price = _price.replace(',', '')
                    if '-' in _price:
                        price, max_price = [p.strip() for p in _price.split('-')]
                        price = ''.join(re.findall(r'\d+\.?\d*', price))
                        max_price = ''.join(re.findall(r'\d+\.?\d*', max_price))
                    else:
                        price = _price
                        price = ''.join(re.findall(r'\d+\.?\d*', price))
                        max_price = 0
                else:
                    price = 0
                    max_price = 0

                price = float(price)
                max_price = float(max_price)
            except:
                price = 0
                max_price = 0
            new_mp = {'page_url': product_url, 'entry': 1, 'rank': rank, 'price': price, 'uuid': _uuid,
                      'max_price': max_price, 'category_info': category, 'category_url': category_url,
                      'category_entry': entry, 'task_id': task_id}
            rds.rc.lpush(StartUrls, new_mp)
    rds.remove_member(CrawlUrls, mp)

    # 判断是否有下一页
    current_page = sel.xpath('//ol[starts-with(@class, "zg_pagination")]/li[contains(@class, "zg_page zg_selected")]/a/@page')
    if current_page:
        current_page_num = current_page[0].strip()
        if current_page_num.isdigit():
            next_page_id = int(current_page_num) + 1
            next_page = sel.xpath('//ol[starts-with(@class, "zg_pagination")]/li/a[@page="%s"]/@href' % next_page_id)
            if next_page:
                next_page_url = next_page[0].strip()
                mapping['page_url'] = next_page_url
                mapping['new'] = 'n'
                rds.rc.lpush(StartUrls, mapping)
            else:
                rds.delete_key(TopRank)


def parse_top_new(html, mp, rds):
    mapping = eval(mp)
    page_url = mapping['page_url']
    entry = mapping['entry']
    task_id = mapping['task_id']
    category_url = mapping.get('category_url', None)
    task_category = mapping.get('task_category', None)
    if not category_url:
        category_url = page_url
        mapping['category_url'] = page_url

    suffix = re.findall(r'www.amazon.(.*?)/', page_url)[0]
    domain = SiteType[suffix]
    sign = domain['sign']
    currency = domain['currency']
    sel = etree.HTML(html)

    category = sel.xpath('//h1/span[@class="category"]/text()')
    if category:
        category = category[0].strip()
    else:
        category = ''
    if task_category:
        category = task_category
    products_lst_3 = sel.xpath('//ol[starts-with(@id, "zg-ordered-list")]/li')
    if products_lst_3:
        products_lst = products_lst_3
    else:
        products_lst = ''
    for pl in products_lst:
        product_url = pl.xpath('.//a[contains(@class,"a-link-normal")]/@href')
        if product_url:
            asin = re.findall(r'/dp/(.+)/ref', product_url[0])
            if asin:
                asin = asin[0].strip()
            else:
                continue
            rank = pl.xpath('.//span[contains(text(), "#")]/text()')
            if rank:
                rank = rank[0].strip().replace('#', '')
            else:
                rank = ''
            try:
                rank = int(rank)
            except ValueError:
                print('rank ValueError')
                continue
            # 处理重复rank和超100排名
            if rds.is_member(TopRank, rank) or rank > 100:
                continue
            rds.add_set(TopRank, rank)

            # 插入关联表
            product_url = 'https://www.amazon.{}/dp/{}'.format(suffix, asin)
            _uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, asin + suffix)).replace('-', '')
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            relevance_mp = {"wtr_sku_uuid": _uuid, "wtr_sku_rank": rank, "wtr_sku_url": product_url,
                            "wtr_task_id": task_id, "wtr_task_type": entry, "wtr_task_info": category,
                            "wtr_platform": 'amazon', "wtr_crawl_time": crawl_time, "wtr_create_time": create_time}
            data_mp = {"table": RelevanceTable, "data": relevance_mp}
            push_data_into_redis(rds, data_mp)

            try:
                price_1 = pl.xpath('.//span[starts-with(@class, "a-size-base a-color-price")]/span/text()')
                if price_1:
                    _price = ''.join(price_1).replace(sign, '').replace(currency, '').replace(' ', '').replace('\xa0', '')
                    if currency == 'EUR':
                        _price = _price.replace('.', '').replace(',', '.')
                    else:
                        _price = _price.replace(',', '')
                    if '-' in _price:
                        price, max_price = [p.strip() for p in _price.split('-')]
                        price = ''.join(re.findall(r'\d+\.?\d*', price))
                        max_price = ''.join(re.findall(r'\d+\.?\d*', max_price))
                    else:
                        price = _price
                        price = ''.join(re.findall(r'\d+\.?\d*', price))
                        max_price = 0
                else:
                    price = 0
                    max_price = 0

                price = float(price)
                max_price = float(max_price)
            except:
                price = 0
                max_price = 0
            new_mp = {'page_url': product_url, 'entry': 1, 'rank': rank, 'price': price, 'uuid': _uuid,
                      'max_price': max_price, 'category_info': category, 'category_url': category_url,
                      'category_entry': entry, 'task_id': task_id}
            rds.rc.lpush(StartUrls, new_mp)
    rds.remove_member(CrawlUrls, mp)

    # 判断是否有下一页
    next_page = sel.xpath('//ul[@class="a-pagination"]/li[@class="a-last"]/a/@href')
    if next_page:
        next_page_url = next_page[0].strip()
        mapping['page_url'] = next_page_url
        mapping['new'] = 'y'
        rds.rc.lpush(StartUrls, mapping)
    else:
        rds.delete_key(TopRank)


# 解析产品页
def parse_product(html, mp, rds):
    mapping = eval(mp)   # eval()函数还原存入字典值的类型
    page_url = mapping['page_url']
    entry = mapping['entry']
    category_info = mapping.get('category_info', '')
    category_url = mapping.get('category_url', '')
    category_entry = mapping.get('category_entry', entry)
    search_box = mapping.get('search_box', '')

    # 确定站点
    suffix = re.findall(r'www.amazon.(.*?)/', page_url)[0]

    # 以下为传入字段
    product_url = page_url
    products_id = re.findall(r'dp/(.+)', page_url)[0]
    this_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, products_id + suffix)).replace('-', '')
    _uuid = mapping.get('uuid', this_uuid)
    sales_total = mapping.get('result_count', '')
    favornum = mapping.get('rank', 0)
    status = mapping.get('task_id', 0)
    if category_entry == 2:
        tags = 'List'
    elif category_entry == 3:
        tags = 'KeyWord'
    elif category_entry == 4:
        tags = 'BestSellers'
    elif category_entry == 5:
        tags = 'NewReleases'
    else:
        tags = 'Detail'

    # 以下为页面解析字段
    product = ParseProduct(html, suffix)

    _name = product.get_title()
    # if not _name:   # 舍弃没有标题的产品
    #     rds.remove_member(CrawlUrls, mp)
    #     collect_error(mp, rds, error='No product name')
    #     return

    currency = product.get_currency()

    first_title = product.get_first_title()

    second_title = ''
    if category_entry == 1:
        category = first_title
    elif category_entry == 3:
        second_title = category_info
        if search_box:
            category = search_box
        else:
            category = first_title
    else:
        category = category_info

    url_id = product.get_asin()

    brand = product.get_brand()

    discount = product.get_discount()

    original_price = product.get_original_price()
    if int(original_price) == 0:
        original_price = mapping.get('original_price', 0)

    price, max_price = product.get_price_maxprice()
    if int(price) == 0:
        price = mapping.get('price', 0)
    if int(max_price) == 0:
        max_price = mapping.get('max_price', 0)

    grade_count = product.get_grade_count()

    review_count = product.get_review_count()

    questions = product.get_questions()

    attribute = product.get_attribute()

    main_image_url = product.get_main_image()

    extra_image_urls = product.get_extra_images()

    if not main_image_url and extra_image_urls:
        main_image_url = extra_image_urls.split(',')[0]

    description = product.get_description()

    generation_time = product.get_generations_time()

    shop_name, shop_url = product.get_shop()

    reserve_field_1 = product.get_reserve_1()

    reserve_field_2 = product.get_reserve_2()

    reserve_field_3 = product.get_reserve_3()

    reserve_field_4 = product.get_reserve_4()

    reserve_field_5 = product.get_reserve_5()

    reserve_field_6, reserve_field_7 = product.get_reserve_6_7()

    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    # 以下为无需处理字段
    platform = 'amazon'
    platform_url = suffix
    dispatch = ''
    shipping = ''
    version_urls = ''
    total_inventory = ''
    is_delete = 0

    # 采集结果缓存进redis
    if category_entry == DetailTag:
        relevance_mp = {"wtr_sku_uuid": _uuid, "wtr_sku_rank": 0, "wtr_sku_url": product_url,
                        "wtr_task_id": status, "wtr_task_type": 1, "wtr_task_info": '',
                        "wtr_platform": 'amazon', "wtr_crawl_time": crawl_time, "wtr_create_time": create_time}
        data_mp = {"table": RelevanceTable, "data": relevance_mp}
        push_data_into_redis(rds, data_mp)

    if category_entry in (BestSellersTag, NewReleasesTag):
        table_name = TrackTable
    else:
        table_name = SkuTable

    sku_mp = {
        'scgs_uuid': _uuid,
        'scgs_products_id': products_id,
        'scgs_url_id': url_id,
        'scgs_brand': brand,
        'scgs_product_url': product_url,
        'scgs_name': _name,
        'scgs_firstTitle': first_title,
        'scgs_secondTitle': second_title,
        'scgs_original_price': original_price,
        'scgs_price': price,
        'scgs_max_price': max_price,
        'scgs_discount': discount,
        'scgs_dispatch': dispatch,
        'scgs_shipping': shipping,
        'scgs_currency': currency,
        'scgs_attribute': attribute,
        'scgs_version_urls': version_urls,
        'scgs_review_count': review_count,
        'scgs_grade_count': grade_count,
        'scgs_sales_total': sales_total,
        'scgs_total_inventory': total_inventory,
        'scgs_favornum': favornum,
        'scgs_image_url': main_image_url,
        'scgs_extra_image_urls': extra_image_urls,
        'scgs_description': description,
        'scgs_category': category,
        'scgs_category_url': category_url,
        'scgs_tags': tags,
        'scgs_shop_name': shop_name,
        'scgs_shop_url': shop_url,
        'scgs_generation_time': generation_time,
        'scgs_platform': platform,
        'scgs_platform_url': platform_url,
        'scgs_crawl_time': crawl_time,
        'scgs_create_time': create_time,
        'scgs_status': status,
        'scgs_questions': questions,
        'scgs_is_delete': is_delete,
        'scgs_reserve_field_1': reserve_field_1,
        'scgs_reserve_field_2': reserve_field_2,
        'scgs_reserve_field_3': reserve_field_3,
        'scgs_reserve_field_4': reserve_field_4,
        'scgs_reserve_field_5': reserve_field_5,
        'scgs_reserve_field_6': reserve_field_6,
        'scgs_reserve_field_7': reserve_field_7,
    }
    data_mp = {"table": table_name, "data": sku_mp}
    push_data_into_redis(rds, data_mp)
    # data_json = json.dumps(data_mp)
    # rds.rc.lpush(MysqlDataList, data_json)
    rds.remove_member(CrawlUrls, mp)



