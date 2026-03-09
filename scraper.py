import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.uraraka-soudan.com"


def get_counselor_list(page=1):
    url = f"{BASE_URL}/counselors?page={page}"

    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")

    counselors = []

    cards = soup.select(".counselor-card")

    for rank, card in enumerate(cards, start=1):

        name = card.select_one(".name").text.strip()
        profile_url = BASE_URL + card.select_one("a")["href"]

        counselors.append({
            "name": name,
            "profile_url": profile_url,
            "display_order": rank
        })

    return counselors
