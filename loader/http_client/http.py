import time
import requests
import logging
from typing import Generator, Any, Union, TypedDict
from abc import abstractmethod, ABC

from .authenticator import Authenticator
from .errors import MethodNotAllowedError

BODY_REQUEST_METHODS = ("GET", "POST", "PUT", "PATCH")


class Configuration(TypedDict):
    url: str
    apikey: str
    params: dict[str, str]


class HTTPClient(ABC):
    _DEFAULT_MAX_RETRY: int = 5

    def __init__(self, config: Configuration):
        self._session = requests.Session()
        self.authenticator = Authenticator(config.get("apikey"))
        self._url = config.get("url")
        self._params = config.get("params")
        self.logger = logging.getLogger("HTTPClient")

    @property
    def http_method(self) -> str:
        return "GET"

    @property
    def url(self) -> str:
        return self._url

    @property
    def params(self) -> dict[str, str]:
        """
        Returns the parameters passed to the Class when initialized. Can be used to set default parameters or change the behavior of the stream.
        """
        return self._params

    @abstractmethod
    def path(self) -> str:
        """
        Returns the path to be used in the request
        """
        pass

    @abstractmethod
    def next_page(self, response: requests.Response) -> Union[str, None]:
        """
        Contains the logic to infer the next page from the response. Can be used either to extract the next page URL or to extract the next page parameters.
        """
        pass

    @abstractmethod
    def get_headers(self, next_page: dict[str, str]) -> dict[str, Any]:
        """
        Returns the headers to be used in the request
        """
        pass

    @abstractmethod
    def get_params(self, next_page: dict[str, str]) -> dict[str, Any]:
        """
        Returns the parameters to be used in the request
        """
        pass

    def prepare_request(self, next_page: str = None) -> requests.PreparedRequest:
        """
        Prepares a request to be sent to the API
        """
        if self.http_method not in BODY_REQUEST_METHODS:
            raise MethodNotAllowedError(f"Method {self.http_method} not allowed")
        request_args = {
            "method": self.http_method,
            "url": self.path(),
            "headers": self.get_headers(next_page),
            "params": self.get_params(next_page),
        }
        return self._session.prepare_request(requests.Request(**request_args))

    def send_request(self, next_page: str = None) -> requests.Response:
        """
        Sends a request to the API with exponential backoff and retries
        """
        request = self.prepare_request(next_page)
        retries = 0
        while retries < self._DEFAULT_MAX_RETRY:
            try:
                response = self._session.send(request)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                retries += 1
                retry_after = 2**retries  # Exponential backoff
                if retries >= self._DEFAULT_MAX_RETRY:
                    self.logger.error(f"Request failed after {retries} retries: {e}.")
                    raise SystemExit(e)
                if e.response.status_code == 429:
                    retry_after = 1 / 5  # 5 requests per second
                    self.logger.warning(
                        f"Rate limit exceeded. Retrying after {retry_after} seconds."
                    )
                elif e.response.status_code == 401:
                    self.logger.error("Unauthorized request. Check your API key.")
                    raise SystemExit(e)
                else:
                    self.logger.warning(
                        f"Request failed with error: {e}. Retrying after {retry_after} seconds."
                    )
                time.sleep(retry_after)

    def read_pages(self) -> Generator[requests.Response, None, None]:
        """
        Reads pages from the API until the end of the stream.
        """
        end_of_stream = False

        next_page = None
        while not end_of_stream:
            response = self.send_request(next_page)
            yield response

            next_page = self.next_page(response)

            if not next_page:
                end_of_stream = True

        yield from []
