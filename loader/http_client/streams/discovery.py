from requests import Response, utils
from urllib import parse
from ..http import HTTPClient


class TicketMaster(HTTPClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_headers(self, next_page: dict[str, str]) -> dict[str, str]:
        return {}

    def get_params(self, next_page: dict[str, str]) -> dict[str, str]:
        params = {
            "size": self.params.get("size", 200),
            "sort": self.params.get("sort", "date,name,asc"),
            "startDateTime": self.params.get("startDateTime", "2022-01-01T00:00:00Z"),
        }
        params.update(self.authenticator.get_params())
        params.update(self.params)
        if next_page:
            params.update(next_page)
        return params

    def next_page(self, response: Response) -> dict[str, str]:
        json_response = response.json()
        links = json_response.get("_links")
        is_next_page_valid = (
            json_response.get("page").get("size")
            * (json_response.get("page").get("number") + 1)
            < 1000
        )  # (page * size) must be less than 1,000 if not error 400 will be raised according to DIS1035
        if links and links.get("next") and is_next_page_valid:
            href = links.get("next").get("href")
            return dict(parse.parse_qsl(parse.urlsplit(href).query))
        else:  # We need to get the next page by using new pagination
            return {
                "startDateTime": json_response.get("_embedded")
                .get("events")[-1]
                .get("dates")
                .get("start")
                .get("dateTime"),
            }
        return None


class EventsStream(TicketMaster):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def path(self) -> str:
        return self.url + "/events.json"


class VenuesStream(TicketMaster):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def path(self) -> str:
        return self.url + "/venues.json"


class AttractionsStream(TicketMaster):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def path(self) -> str:
        return self.url + "/attractions.json"
