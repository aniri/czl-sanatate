# -*- coding: utf-8 -*-

import scrapy
import re
import scraperwiki
from scrapy.crawler import CrawlerProcess
import logging
import hashlib
from unidecode import unidecode
import json
from datetime import datetime, timedelta

# TODO: check wrong data example : Afisat de la 03-011-2017 pana la 13-11-2017
# Afisat de la 226-01-2017 pana la 08-03-2017

INDEX_URL = "http://www.ms.ro/acte-normative-in-transparenta/?vpage=1"

DIACRITICS_RULES = [
    (ur'[șş]', 's'),
    (ur'[ȘŞ]', 'S'),
    (ur'[țţ]', 't'),
    (ur'[ȚŢ]', 'T'),
    (ur'[ăâ]', 'a'),
    (ur'[ĂÂ]', 'A'),
    (ur'[î]', 'i'),
    (ur'[Î]', 'I'),
]

TYPE_RULES = [
    ("lege", "LEGE"),
    ("hotarare de guvern", "HG"),
    ("hotarare a guvernului", "HG"),
    ("hotarare", "HG"),
    ("hg", "HG"),
    ("ordonanta de guvern", "OG"),
    ("ordonanta de urgenta", "OUG"),
    ("ordin de ministru", "OM"),
    ("ordinul", "OM"),
    ("ordin", "OM"),
    ("ordonanta", "OG"),
]

DATE_PATTERN = re.compile('de\s+la\s+(\d{1,2}[-/]\d{2}[-/]\d{4})')
CONTACT_EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')
CONTACT_TEL_PATTERN = re.compile(r'[^0-9](0(?:[0-9].?){9})')
FEEDBACK_DAYS_PATTERN = re.compile(r'termen.*limita.*[^[0-9]]*([0-9]{1,2}).*zi')

def guess_initiative_type(text, rules):
    """
    Try to identify the type of a law initiative from its description.
    Use a best guess approach. The rules are provided by the caller as a list
    of tuples. Each tuple is composed of a search string and the initiative
    type it matches to.
    :param text: the description of the initiative
    :param rules: the rules of identification expressed as a list of tuples
    :return: the type of initiative if a rule matches; "OTHER" if no rule
    matches
    """
    text = strip_diacritics(text)

    for search_string, initiative_type in rules:
        if search_string in text:
            return initiative_type
    else:
        return "OTHER"

def strip_diacritics(text):
    """
    Replace all diacritics in the given text with their regular counterparts.
    :param text: the text to look into
    :return: the text without diacritics
    """
    result = text
    for search_pattern, replacement in DIACRITICS_RULES:
        result = re.sub(search_pattern, replacement, result, re.UNICODE)
    return unidecode(result)

class Publication(scrapy.Item):
    institution = scrapy.Field()
    identifier = scrapy.Field()
    type = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    documents = scrapy.Field()
    contact = scrapy.Field()
    feedback_days = scrapy.Field()
    max_feedback_date = scrapy.Field()

def identify(title, url):
    return " : ".join([url, hashlib.md5(title).hexdigest()])

class SanatateSpider(scrapy.Spider):

    name = 'sanatate'
    start_urls = [INDEX_URL]

    def parse(self, response):
        logging.warn("scrapping: sanatate - %s"%(response.url))
        for item in response.css('.panel'):
            heading = item.css('div.panel-heading')
            body = item.css('div.panel-body')
            body_text = ''.join(body.xpath('.//text()').extract()).lower()

            title = item.css('a.panel-title::text').extract_first().strip()
            title = strip_diacritics(title)

            # clean up most of the title before checking publication type
            publication_text = title.lower()
            publication_type = "OTHER"
            stop_pos = re.search(r'(pentru|privind)', publication_text)
            if stop_pos:
                publication_text_short = publication_text[0:stop_pos.start()]
                publication_type = guess_initiative_type(publication_text_short, TYPE_RULES)

            contact = self.get_contacts(strip_diacritics(body_text))
            json_contact = json.dumps(contact)

            text_date = DATE_PATTERN.findall(body_text)
            text_feedback_days = FEEDBACK_DAYS_PATTERN.findall(body_text)
            feedback_days = int(text_feedback_days[0])

            date, feedback_date = None, None

            if text_date:
                date, date_obj = self.parse_date(text_date)
                feedback_date = (date_obj + timedelta(days=feedback_days)).date().isoformat()

            keys = ['type', 'url']
            types = body.xpath('.//a[contains(@href, ".pdf")]').xpath('text()').extract()
            urls = body.xpath('.//a[contains(@href, ".pdf")]').xpath('@href').extract()
            docs = [[types[i], urls[i]] for i in range(len(types))]
            documents = [dict(zip(keys, doc)) for doc in docs]

            publication = Publication(
                institution = 'sanatate',
                identifier = identify(title, response.url),
                type = publication_type,
                date = date,
                title = title,
                description = strip_diacritics(body_text),
                documents = json.dumps(documents),
                contact = json_contact,
                feedback_days = feedback_days,
                max_feedback_date = feedback_date
            )

            scraperwiki.sqlite.save(unique_keys=['identifier'], data=dict(publication))

        # check if there is a next page and crawl it too :)
        current_page = int(response.css('.pt-cv-pagination::attr(data-currentpage)').extract_first())
        total_pages = int(response.css('.pt-cv-pagination::attr(data-totalpages)').extract_first())
        next_page = current_page + 1
        if (current_page < total_pages):
            next_page_url = response.url.replace(str(current_page), str(next_page))
            yield scrapy.Request(next_page_url, callback=self.parse)

    def parse_date(self, text):
        for date_text in text:
            try:
                date_obj = datetime.strptime(date_text, '%d-%m-%Y')
                date = date_obj.date().isoformat()
            except ValueError:
                date = None
            return date, date_obj

    def get_contacts(self, text):
        text = text.strip().lower()

        contact = {}

        emails = re.findall(CONTACT_EMAIL_PATTERN, text)
        contact['email'] = list(set(emails))

        numbers = re.findall(CONTACT_TEL_PATTERN, text)
        contact['tel'] = list(set(numbers))

        return contact

process = CrawlerProcess()
process.crawl(SanatateSpider)
process.start()
