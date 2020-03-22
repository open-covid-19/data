#!/usr/bin/env python

import re
import sys
import scrapy

BASE_URL = 'https://www.ecdc.europa.eu'
class WhoSpider(scrapy.Spider):
    name = __name__
    start_urls = ['%s/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide' % BASE_URL]

    def parse(self, response):
        links = []
        for link in response.css('[href]'):
            link = link.xpath("@href").extract_first()
            # print(link)
            if re.match(r'.*.xls', link):
                links.append(link)
        links = sorted(links, key=lambda x: x.split('/')[-1])
        print(list(links)[-1])
