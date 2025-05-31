import logging
import time

logger = logging.getLogger(__name__)

class RequestLoggingMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start_time = time.time()
        logger.info('Request: %s %s %s', request.method, request.path, request.GET)

        response = self.get_response(request)

        duration = time.time() - start_time
        logger.info(
            'Response: %s %s %s %sms',
            request.method,
            request.path,
            response.status_code,
            int(duration * 1000)
        )
        return response
