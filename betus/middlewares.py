from scrapy.downloadermiddlewares.retry import *
from scrapy.spidermiddlewares.httperror import *

from fake_useragent import UserAgent

class Retry500Middleware(RetryMiddleware):

    def __init__(self, settings):
        """Initializes the Retry500Middleware class.
        
        Args:
            settings (dict): A dictionary containing configuration settings for the middleware.
        
        Returns:
            None: This method doesn't return anything.
        """
        super(Retry500Middleware, self).__init__(settings)

        fallback = settings.get('FAKEUSERAGENT_FALLBACK', None)
        """Process the response and retry the request if necessary.
        
        Args:
            request (scrapy.http.Request): The original request object.
            response (scrapy.http.Response): The response object to process.
            spider (scrapy.Spider): The spider instance that generated the request.
        
        Returns:
            scrapy.http.Response: The processed response or a new request object for retry.
        """        self.ua = UserAgent(fallback=fallback)
        self.ua_type = settings.get('RANDOM_UA_TYPE', 'random')

    def get_ua(self):
        '''Gets random UA based on the type setting (random, firefox…)'''
        return getattr(self.ua, self.ua_type)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            """Process an exception that occurred during a request.
            
            Args:
                request (Request): The request object that caused the exception.
                exception (Exception): The exception that occurred.
                spider (Spider): The spider that made the request.
            
            Returns:
                Request or None: A new request object to retry the failed request with a new User-Agent,
                                 or None if the exception should not be retried.
            """
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