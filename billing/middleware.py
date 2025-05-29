import logging

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        logger.info('Request: %s %s', request.method, request.path)
        response = self.get_response(request)
        logger.info('Response: %s %s', request.method, request.path)
        return response
