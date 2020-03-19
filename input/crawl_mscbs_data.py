#!/bin/env python

import re
import sys
import scrapy

BASE_URL = 'https://www.mscbs.gob.es'
class WhoSpider(scrapy.Spider):
    name = __name__
    start_urls = ['%s/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm' % BASE_URL]

    def parse(self, response):
        links = []
        for link in response.css('a'):
            link = link.xpath("@href").extract_first()
            href_regex = r'.*(profesionales/saludPublica/ccayes/alertasActual/nCov-China/documentos/Actualizacion_\d\d_COVID-19.pdf)'
            if re.match(href_regex, link):
                links.append('%s/%s' % ('', re.match(href_regex, link).groups(1)[0]))
        links = sorted(links, key=lambda x: x.split('/')[-1].split('-')[0])
        print('{}{}'.format(BASE_URL, list(links)[-1]))
