"""
Connection Wrapper module
"""
import http.client
import logging
import os
import json


class ConnectionWrapper():
    """
    Wrapper class to re-use existing connection
    """
    def __init__(self, host, timeout=60, keep_open=False):
        self.connection = None
        self.connection_attempts = 3
        self.host_url = host.replace('https://', '').replace('http://', '')
        self.cert_file = None
        self.key_file = None
        self.logger = logging.getLogger('logger')
        self.timeout = timeout
        self.keep_open = keep_open

    def init_connection(self, url):
        """
        Create a new connection
        """
        if not self.cert_file or not self.key_file:
            self.cert_file = os.getenv('USERCRT', None)
            self.key_file = os.getenv('USERKEY', None)

        if not self.cert_file or not self.key_file:
            self.cert_file = os.getenv('X509_USER_PROXY', None)
            self.key_file = os.getenv('X509_USER_PROXY', None)

        if not self.cert_file or not self.key_file:
            raise Exception('Missing USERCRT or USERKEY or X509_USER_PROXY environment variables')

        return http.client.HTTPSConnection(url,
                                           port=443,
                                           cert_file=self.cert_file,
                                           key_file=self.key_file,
                                           timeout=self.timeout)

    def refresh_connection(self, url):
        """
        Recreate a connection
        """
        # self.logger.info('Refreshing connection')
        self.connection = self.init_connection(url)

    def api(self, method, url, data=None):
        """
        Make a HTTP request with given method, url and data
        """
        if not self.connection:
            self.refresh_connection(self.host_url)

        url = url.replace('#', '%23')
        for _ in range(self.connection_attempts):
            try:
                data = json.dumps(data) if data else None
                self.connection.request(method, url, data, headers={'Accept': 'application/json'})
                response = self.connection.getresponse()
                if response.status != 200:
                    self.logger.info('Problems (%d) with [%s] %s: %s',
                                     response.status,
                                     method,
                                     url,
                                     response.read())
                    return None

                response_to_return = response.read()
                if not self.keep_open:
                    self.connection.close()
                    self.connection = None

                return response_to_return

            except Exception as ex:
                self.refresh_connection(self.host_url)

        self.logger.error('Connection wrapper failed after %d attempts', self.connection_attempts)
        return None
