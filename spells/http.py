"""One way to talk to the network.

Every request spells makes goes through here so that the user agent, timeouts,
and retry policy are set in one place rather than per call site.

`download` writes through a `.part` file in the destination directory and
renames it into place, so an interrupted download leaves an obviously partial
file next to its target instead of a plausible-looking complete one.
"""

import os
import threading

import requests
from requests.adapters import HTTPAdapter
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from urllib3.util.retry import Retry

USER_AGENT = "spells-mtg"
TIMEOUT = 30

# S3 and the Prismic CDN are both occasionally flaky, and the nightly DEq run is
# unattended, so idempotent requests get a few automatic attempts.
RETRY = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "HEAD"),
)

_local = threading.local()


def session() -> requests.Session:
    """A Session per thread: `catalog.resolve` HEADs concurrently, and Session
    is not documented as thread-safe."""
    if (existing := getattr(_local, "session", None)) is not None:
        return existing

    new = requests.Session()
    new.headers["User-Agent"] = USER_AGENT
    adapter = HTTPAdapter(max_retries=RETRY)
    new.mount("https://", adapter)
    new.mount("http://", adapter)
    _local.session = new
    return new


def get_json(url: str) -> dict:
    response = session().get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def head(url: str) -> requests.Response | None:
    """None if the object is missing or unreachable.

    17Lands grants `public-read` per object rather than on the bucket, so an
    unpublished dataset answers 403 rather than 404.
    """
    try:
        response = session().head(url, timeout=TIMEOUT, allow_redirects=True)
    except requests.RequestException:
        return None
    return response if response.ok else None


def _progress() -> Progress:
    return Progress(
        TextColumn("  [progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
    )


def download(url: str, path: str, description: str = "", progress: bool = True) -> str:
    """Stream `url` to `path`, returning the path actually written.

    Unlike a bare write, the destination only ever appears once the transfer has
    finished, so a failed run cannot leave a truncated file that later looks
    complete to `spells status`.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    part = f"{path}.part"

    try:
        with session().get(url, stream=True, timeout=TIMEOUT) as response:
            response.raise_for_status()
            total = int(response.headers.get("Content-Length") or 0)

            with open(part, "wb") as f:
                if not progress:
                    for chunk in response.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                else:
                    with _progress() as bar:
                        task = bar.add_task(
                            description or os.path.basename(path), total=total or None
                        )
                        for chunk in response.iter_content(chunk_size=1 << 20):
                            f.write(chunk)
                            bar.advance(task, len(chunk))

        os.replace(part, path)
    except BaseException:
        if os.path.exists(part):
            os.remove(part)
        raise

    return path
