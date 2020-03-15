#!/bin/env python

import re
import sys
import scrapy

BASE_URL = 'https://www.who.int'
# https://www.who.int/docs/default-source/coronaviruse/situation-reports/20200314-sitrep-54-covid-19.pdf?sfvrsn=dcd46351_2
class WhoSpider(scrapy.Spider):
    name = __name__
    start_urls = ['%s/emergencies/diseases/novel-coronavirus-2019/situation-reports' % BASE_URL]

    def parse(self, response):
        links = []
        for link in response.css('a'):
            link = link.xpath("@href").extract_first()
            if re.match(r'.+\d{8}-sitrep-\d\d-covid-19.pdf.*', link) and self.date in link:
                links.append(link)
        links = sorted(links, key=lambda x: x.split('/')[-1].split('-')[0])
        print('{}{}'.format(BASE_URL, list(links)[-1]))