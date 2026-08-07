"""Tests for the shared HTTP layer.

The behaviors asserted here are the ones the previous `wget`-backed download
got wrong: it staged into the *current working directory*, and it silently
renamed to `name (1).ext` when the destination already existed, while callers
went on using the path they had asked for.
"""

import os

import pytest
import requests

from spells import http


class FakeResponse:
    def __init__(self, body=b"", headers=None, ok=True, status=200, json_data=None):
        self.content = body
        self.headers = headers or {}
        self.ok = ok
        self.status_code = status
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError(f"{self.status_code}")

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(("get", url, kwargs))
        if self.error:
            raise self.error
        return self.response

    def head(self, url, **kwargs):
        self.calls.append(("head", url, kwargs))
        if self.error:
            raise self.error
        return self.response


@pytest.fixture
def fake_session(monkeypatch):
    def install(response=None, error=None):
        session = FakeSession(response, error)
        monkeypatch.setattr(http, "session", lambda: session)
        return session

    return install


def test_download_writes_the_exact_path_requested(tmp_path, fake_session):
    fake_session(FakeResponse(b"payload", {"Content-Length": "7"}))
    target = tmp_path / "sets" / "data.csv.gz"

    written = http.download("https://example/x", str(target), progress=False)

    assert written == str(target)
    assert target.read_bytes() == b"payload"


def test_download_overwrites_rather_than_renaming_alongside(tmp_path, fake_session):
    """wget produced `data (1).csv.gz` here and returned it, while the caller
    kept reading the stale file it had asked for."""
    fake_session(FakeResponse(b"new", {"Content-Length": "3"}))
    target = tmp_path / "data.csv.gz"
    target.write_bytes(b"stale")

    http.download("https://example/x", str(target), progress=False)

    assert target.read_bytes() == b"new"
    assert [p.name for p in tmp_path.iterdir()] == ["data.csv.gz"]


def test_download_stages_inside_the_destination_directory(tmp_path, monkeypatch):
    """wget staged its temp file in the cwd, littering whatever directory the
    command happened to run from."""
    seen = {}

    class WatchingResponse(FakeResponse):
        def iter_content(self, chunk_size=1):
            seen["cwd"] = sorted(os.listdir("."))
            seen["dest"] = sorted(os.listdir(tmp_path / "sets"))
            return super().iter_content(chunk_size)

    session = FakeSession(WatchingResponse(b"payload", {"Content-Length": "7"}))
    monkeypatch.setattr(http, "session", lambda: session)

    run_dir = tmp_path / "elsewhere"
    run_dir.mkdir()
    monkeypatch.chdir(run_dir)

    http.download("https://example/x", str(tmp_path / "sets" / "d.gz"), progress=False)

    assert seen["cwd"] == []
    assert seen["dest"] == ["d.gz.part"]


def test_download_leaves_no_file_behind_when_the_transfer_fails(tmp_path, fake_session):
    class Exploding(FakeResponse):
        def iter_content(self, chunk_size=1):
            yield b"half"
            raise requests.ConnectionError("dropped")

    fake_session(Exploding(b"", {"Content-Length": "8"}))
    target = tmp_path / "data.csv.gz"

    with pytest.raises(requests.ConnectionError):
        http.download("https://example/x", str(target), progress=False)

    assert not target.exists()
    assert list(tmp_path.iterdir()) == []


def test_download_does_not_clobber_the_target_on_failure(tmp_path, fake_session):
    class Exploding(FakeResponse):
        def iter_content(self, chunk_size=1):
            raise requests.ConnectionError("dropped")
            yield

    fake_session(Exploding(b"", {"Content-Length": "8"}))
    target = tmp_path / "data.csv.gz"
    target.write_bytes(b"good data")

    with pytest.raises(requests.ConnectionError):
        http.download("https://example/x", str(target), progress=False)

    assert target.read_bytes() == b"good data"


def test_download_raises_on_an_error_status(tmp_path, fake_session):
    fake_session(FakeResponse(b"", ok=False, status=403))
    target = tmp_path / "data.csv.gz"

    with pytest.raises(requests.HTTPError):
        http.download("https://example/x", str(target), progress=False)

    assert not target.exists()


def test_head_returns_none_for_an_unpublished_object(fake_session):
    fake_session(FakeResponse(ok=False, status=403))
    assert http.head("https://example/missing") is None


def test_head_returns_none_when_unreachable(fake_session):
    fake_session(error=requests.ConnectionError("no network"))
    assert http.head("https://example/x") is None


def test_head_returns_the_response_when_published(fake_session):
    fake_session(FakeResponse(headers={"Content-Length": "10"}))
    response = http.head("https://example/x")
    assert response is not None
    assert response.headers["Content-Length"] == "10"


def test_get_json_returns_the_decoded_body(fake_session):
    fake_session(FakeResponse(json_data={"refs": [{"ref": "abc"}]}))
    assert http.get_json("https://example/api")["refs"][0]["ref"] == "abc"


def test_get_json_raises_on_an_error_status(fake_session):
    fake_session(FakeResponse(ok=False, status=500))
    with pytest.raises(requests.HTTPError):
        http.get_json("https://example/api")


def test_requests_carry_a_timeout(tmp_path, fake_session):
    session = fake_session(FakeResponse(json_data={}))
    http.get_json("https://example/api")
    assert session.calls[0][2]["timeout"] == http.TIMEOUT


def test_sessions_are_per_thread():
    """catalog.resolve HEADs concurrently, and Session is not thread-safe.

    A barrier forces both threads to be live at once; a plain thread pool is
    free to run two quick tasks on the same worker and would prove nothing.
    """
    import threading

    barrier = threading.Barrier(2)
    ids = []
    lock = threading.Lock()

    def record():
        barrier.wait(timeout=5)
        with lock:
            ids.append(id(http.session()))

    threads = [threading.Thread(target=record) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert len(set(ids)) == 2


def test_session_is_reused_within_one_thread():
    assert http.session() is http.session()


def test_session_sets_the_user_agent():
    assert http.session().headers["User-Agent"] == http.USER_AGENT
