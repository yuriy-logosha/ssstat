import time
from datetime import timedelta, date, datetime

import pymongo
from bson import ObjectId

local = "192.168.1.61"
db = "ss_ads"
default = f"mongodb://{local}:27017/?authSource={db}"
myclient = pymongo.MongoClient(default)
begin_day = lambda day: datetime(day.year, day.month, day.day, 0, 0, 0)

with myclient:
    ss_stat = myclient.ss_ads['stat']
    ss_ads = myclient.ss_ads.ads
    geodata = myclient.ss_ads.geodata
    kind_ad = {'kind': 'ad'}
    kind_old_price = {'kind': 'old_price'}

    while True:
        ads = list(ss_ads.find(kind_ad))

        today_id = ObjectId.from_datetime(begin_day(datetime.now()))
        ads_today = list(ss_ads.find({'$and': [{'kind': 'ad'}, {"_id": {"$gt": today_id}}]}))

        yesterday_id = ObjectId.from_datetime(begin_day(date.today() - timedelta(days=1)))
        ads_yesterday = list(
            ss_ads.find({'$and': [{'kind': 'ad'}, {"_id": {"$gt": yesterday_id}}, {"_id": {"$lt": today_id}}]}))

        d_b_yesterday_id = ObjectId.from_datetime(begin_day(date.today() - timedelta(days=2)))
        d_b_ads_yesterday = list(
            ss_ads.find({'$and': [{'kind': 'ad'}, {"_id": {"$gt": d_b_yesterday_id}}, {"_id": {"$lt": yesterday_id}}]}))

        geodata_today = list(geodata.find({"_id": {"$gt": today_id}}))
        houses = len(list(ss_ads.find({'kind': 'ad', 'type': 'Ч. дом'})))
        geo_address = list(geodata.distinct('address', {}))
        geo_address_empty = set(geodata.distinct('address', filter={"geodata": []}))
        total_address = list(ss_ads.distinct("address_lv", kind_ad))
        diff_prices = list(ss_ads.find(kind_old_price))

        print("Время:", datetime.now())
        print("Объявлений: Всего/позавчера/вчера/сегодня ", len(ads), len(d_b_ads_yesterday), len(ads_yesterday),
              len(ads_today))
        print("Всего домов:", houses)
        print("Получено геоданных адресов всего/сегодня:", len(geo_address), len(geodata_today))
        diff_geo = list(set(total_address) - set(geo_address))
        print("Необходимо обработать адресов:", len(diff_geo), diff_geo)
        print("Адресов без геоданных:", len(geo_address_empty), geo_address_empty)
        summ = sum(int(ad['price'].replace('€', '').replace(',', '')) for ad in ads)
        print("Всего евро: %s €" % summ)
        print("Всего изменений цены:", len(diff_prices))
        r = ss_stat.insert_one({'ads_count': len(ads), 'houses': houses, 'geo_address': len(geo_address),
                                'total_address': len(total_address), 'diff_prices': len(diff_prices),
                                'diff_geo': len(diff_geo), 'ads_today': len(ads_today),
                                'geodata_today': len(geodata_today), 'empty_geodata': len(geo_address_empty),
                                'total_eur': summ, 'date': datetime.now()})

        [print(i) for i in ss_stat.find().sort([('date', -1)]).limit(2)]
        ads = []
        ads_today = []
        ads_yesterday = []
        d_b_ads_yesterday = []
        geodata_today = []
        geo_address = []
        total_address = []
        diff_prices = []

        time.sleep(900)
