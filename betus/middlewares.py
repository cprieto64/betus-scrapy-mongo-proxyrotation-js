from scrapy.downloadermiddlewares.retry import *
from scrapy.spidermiddlewares.httperror import *

from fake_useragent import UserAgent

class Retry500Middleware(RetryMiddleware):

    def __init__(self, settings):
        super(Retry500Middleware, self).__init__(settings)

        fallback = settings.get('FAKEUSERAGENT_FALLBACK', None)
        self.ua = UserAgent(fallback=fallback)
        self.ua_type = settings.get('RANDOM_UA_TYPE', 'random')

    def get_ua(self):
        '''Gets random UA based on the type setting (random, firefox…)'''
        return getattr(self.ua, self.ua_type)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            request.headers['User-Agent'] = self.get_ua()
            return self._retry(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) \
                and not request.meta.get('dont_retry', False):
            request.headers['User-Agent'] = self.get_ua()
            return self._retry(request, exception, spider)