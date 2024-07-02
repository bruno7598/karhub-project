from pytest import mark
from modules.utils import request_api_dollar


@mark.parametrize(
    ("input", "output"),
    [
        ("https://economia.awesomeapi.com.br/json/daily/USD-BRL?start_date=20220622&end_date=20220622", 5.1945),
    ],
)
def test_request_api_dollar(input, output):
    assert request_api_dollar(input) == output
