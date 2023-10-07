import asyncio
from typing import AsyncIterable, Sequence

import pandas as pd
from aiohttp import ClientSession
from bs4 import BeautifulSoup

START_DATE = pd.Timestamp("2011-12-05", tz="Asia/Tokyo")


def generate_racecard_url(month: int, day: int) -> str:
    url = (
        "https://keirin.kdreams.jp/racecard/2022/"
        + str(month).zfill(2)
        + "/"
        + str(day).zfill(2)
        + "/"
    )
    return url


def get_racecard_url() -> Sequence[str]:
    current_date = pd.Timestamp.now(tz="Asia/Tokyo")

    # get all dates between START_DATE and current_date
    dates = pd.date_range(START_DATE, current_date)
    return [generate_racecard_url(month=date.month, day=date.day) for date in dates]


async def get_race_urls(session: ClientSession, url: str) -> Sequence[str]:
    request = await session.get(url)
    htm = BeautifulSoup(await request.text(), "html.parser")

    # get all <a> tags
    urls = []
    anchors = htm.find_all("a")
    for anchor in anchors:
        url = anchor.get("href")
        if "racedetail" in url:
            urls.append(url)
    return urls


async def get_all_race_urls(session: ClientSession, delay: float) -> AsyncIterable[str]:
    racecard_urls = get_racecard_url()
    for racecard_url in racecard_urls:
        await asyncio.sleep(delay)
        for race_url in await get_race_urls(session, racecard_url):
            yield race_url
