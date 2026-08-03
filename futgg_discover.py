"""Opt-in diagnostic: find where FUT.GG player price data actually comes from.

WHY THIS EXISTS
---------------
The price worker currently loads a full player page in Chromium, waits for
the client-side render to settle, serialises the DOM and parses ~275 KB of
HTML - roughly 9 seconds of wall time per card, of which measurement shows
~96% is the page loading and hydrating rather than anything the automation
harness does. The structure of the worker itself is evidence that the price
data arrives asynchronously after hydration: there would be no need for a
12-second wait on #prices-overview, a 700ms unconditional settle, a 2000ms
retry settle, or a "price_section_missing" outcome if the markup were
server-rendered complete.

If the page fetches its prices from a JSON endpoint after hydration, then we
are executing an entire React application in order to obtain a payload we
could request directly. This diagnostic establishes whether that is true,
WITHOUT changing the production architecture.

It answers one question, classifying the source as:

    A. reusable JSON/XHR endpoint
    B. GraphQL
    C. embedded page JSON (__NEXT_DATA__ / RSC / script[type=application/json])
    D. rendered HTML only (no reusable payload - browser rendering required)

SAFETY
------
- Entirely opt-in: FUTGG_DISCOVER_PRICE_NETWORK must be explicitly true.
  When false (the default) this module is never imported by the worker.
- One page, one URL, one navigation. It is not a crawler.
- Nothing that could re-authenticate a session is logged. Cookies,
  authorization headers and credential-shaped strings are redacted by
  futgg_instrumentation.redact_* before anything reaches the log.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from futgg_instrumentation import redact_headers, redact_structure, redact_url

log = logging.getLogger("futgg_discover")


# Keys/paths that indicate a payload carries the data we need.
_INTERESTING_KEY_MARKERS = (
    "price",
    "bin",
    "buynow",
    "buy_now",
    "sale",
    "sold",
    "market",
    "auction",
    "lowest",
    "listing",
)

# URL fragments that mark a request as a candidate data source regardless
# of how Playwright classified its resource type.
_INTERESTING_URL_MARKERS = (
    "/api/",
    "/graphql",
    "/_next/data/",
    "price",
    "market",
    "sale",
    "player",
)

_STATIC_ASSET = re.compile(
    r"\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|eot|css|mp4|webm)(\?|$)",
    re.IGNORECASE,
)

MAX_BODY_BYTES_TO_INSPECT = 2_000_000


def _url_is_interesting(url: str) -> bool:
    if _STATIC_ASSET.search(url):
        return False
    lowered = url.lower()
    return any(marker in lowered for marker in _INTERESTING_URL_MARKERS)


def _find_interesting_keys(value: Any, *, depth: int = 0, found: set[str] | None = None) -> set[str]:
    """Recursively look for price/sales-shaped keys anywhere in a payload.

    Depth-limited: the point is to classify the payload, not to walk a
    megabyte of RSC data exhaustively.
    """
    if found is None:
        found = set()
    if depth > 6:
        return found
    if isinstance(value, dict):
        for key, sub in value.items():
            lowered = str(key).lower()
            if any(marker in lowered for marker in _INTERESTING_KEY_MARKERS):
                found.add(str(key))
            _find_interesting_keys(sub, depth=depth + 1, found=found)
    elif isinstance(value, list):
        for item in value[:20]:
            _find_interesting_keys(item, depth=depth + 1, found=found)
    return found


class NetworkDiscovery:
    """Collects and classifies responses seen during one page load."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.responses: list[dict[str, Any]] = []
        self.embedded: list[dict[str, Any]] = []
        self._pending: list[asyncio.Task] = []

    def attach(self, page) -> None:
        """Attach the listener BEFORE navigation.

        Bodies are read in a background task: the response handler runs on
        the same event loop that drives the navigation, so awaiting a body
        inline would stall the very page load being measured.
        """
        page.on("response", self._on_response)

    def _on_response(self, response) -> None:
        try:
            request = response.request
        except Exception:
            return
        resource_type = getattr(request, "resource_type", "") or ""
        url = response.url

        is_candidate = (
            resource_type in {"fetch", "xhr", "document"}
            or _url_is_interesting(url)
        )
        if not is_candidate:
            return

        self._pending.append(asyncio.ensure_future(self._capture(response, request)))

    async def _capture(self, response, request) -> None:
        entry: dict[str, Any] = {
            "method": request.method,
            # Redacted at capture: the signed price URL carries its verify
            # token in the query string, which header/body redaction never
            # touches.
            "url": redact_url(response.url),
            "status": response.status,
            "resource_type": getattr(request, "resource_type", None),
        }
        try:
            headers = await response.all_headers()
        except Exception:
            headers = {}
        content_type = (headers.get("content-type") or "").lower()
        entry["response_content_type"] = content_type
        entry["response_content_length"] = headers.get("content-length")

        try:
            entry["request_headers"] = redact_headers(await request.all_headers())
        except Exception:
            entry["request_headers"] = {}

        post_data = None
        try:
            post_data = request.post_data
        except Exception:
            post_data = None
        if post_data:
            entry["post_body_bytes"] = len(post_data)
            try:
                entry["post_body_preview"] = redact_structure(json.loads(post_data))
            except Exception:
                entry["post_body_preview"] = post_data[:300]
            entry["is_graphql"] = (
                "graphql" in response.url.lower() or '"query"' in post_data
            )

        body_bytes = b""
        try:
            body_bytes = await response.body()
        except Exception:
            # Redirects, aborted requests and cached responses legitimately
            # have no retrievable body - not an error worth failing over.
            entry["body_unavailable"] = True

        entry["body_size_bytes"] = len(body_bytes)
        entry["is_json"] = False

        if body_bytes and len(body_bytes) <= MAX_BODY_BYTES_TO_INSPECT:
            looks_json = "json" in content_type or body_bytes[:1] in (b"{", b"[")
            if looks_json:
                try:
                    parsed = json.loads(body_bytes)
                    entry["is_json"] = True
                    entry["top_level_keys"] = (
                        list(parsed)[:40] if isinstance(parsed, dict)
                        else f"<list:{len(parsed)} items>"
                    )
                    interesting = _find_interesting_keys(parsed)
                    entry["interesting_keys"] = sorted(interesting)[:40]
                    entry["carries_market_data"] = bool(interesting)
                    entry["preview"] = redact_structure(parsed)
                except Exception:
                    entry["is_json"] = False

        self.responses.append(entry)

    async def drain(self, timeout: float = 10.0) -> None:
        """Wait for in-flight body reads to finish."""
        if not self._pending:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._pending, return_exceptions=True), timeout=timeout
            )
        except asyncio.TimeoutError:
            log.warning("timed out draining %d pending body reads", len(self._pending))


async def inspect_dom_candidates(page, price_selector: str) -> dict[str, Any]:
    """Find what the price container is called NOW.

    Added in response to a production outage in which every card failed
    with `price_section_missing`: the page loaded, `.fc-card` rendered, but
    `#prices-overview` was absent. That is either a markup rename on
    FUT.GG's side or a container that only mounts when scrolled into view.
    Guessing a replacement selector would be speculation, so this reports
    every element that plausibly holds price data and lets the evidence
    decide.

    Also probes for bot-interstitial markers, which is the cheapest way to
    rule out "we are being served a challenge page" as the cause.
    """
    script = """
    (priceSelector) => {
        const markers = ['price','market','bin','sale','listing','value','coin'];
        const out = {
            title: document.title,
            price_selector_present: !!document.querySelector(priceSelector),
            fc_card_count: document.querySelectorAll('.fc-card').length,
            body_text_length: (document.body ? document.body.innerText.length : 0),
            scroll_height: document.documentElement.scrollHeight,
            viewport_height: window.innerHeight,
            candidates: [],
            challenge_markers: [],
        };
        const lowerBody = (document.body ? document.body.innerText : '').toLowerCase();
        for (const phrase of ['just a moment','checking your browser',
                              'verify you are human','enable javascript and cookies',
                              'attention required']) {
            if (lowerBody.includes(phrase)) out.challenge_markers.push(phrase);
        }
        const seen = new Set();
        for (const el of document.querySelectorAll('[id],[class],[data-testid]')) {
            const id = el.id || '';
            const cls = (typeof el.className === 'string' ? el.className : '') || '';
            const testid = el.getAttribute('data-testid') || '';
            const hay = (id + ' ' + cls + ' ' + testid).toLowerCase();
            if (!markers.some(m => hay.includes(m))) continue;
            const key = el.tagName + '|' + id + '|' + cls.slice(0, 60);
            if (seen.has(key)) continue;
            seen.add(key);
            const text = (el.innerText || '').trim().replace(/\\s+/g, ' ');
            out.candidates.push({
                tag: el.tagName.toLowerCase(),
                id: id || null,
                class: cls.slice(0, 120) || null,
                testid: testid || null,
                text_length: text.length,
                text_preview: text.slice(0, 160),
                has_digits: /\\d[\\d,.]{2,}/.test(text),
            });
            if (out.candidates.length >= 60) break;
        }
        return out;
    }
    """
    try:
        return await page.evaluate(script, price_selector)
    except Exception:
        log.exception("discovery: DOM candidate inspection failed")
        return {}


async def inspect_embedded_json(page) -> list[dict[str, Any]]:
    """Look for data baked into the HTML itself.

    Covers three shapes: the classic Next.js __NEXT_DATA__ blob, any
    script[type="application/json"] payload, and React Server Component
    streaming payloads (self.__next_f), which is how current Next.js App
    Router pages ship their data.
    """
    findings: list[dict[str, Any]] = []

    try:
        next_data = await page.evaluate(
            "() => window.__NEXT_DATA__ ? JSON.stringify(window.__NEXT_DATA__) : null"
        )
    except Exception:
        next_data = None
    if next_data:
        entry: dict[str, Any] = {"source": "window.__NEXT_DATA__", "size_bytes": len(next_data)}
        try:
            parsed = json.loads(next_data)
            entry["top_level_keys"] = list(parsed)[:40]
            interesting = _find_interesting_keys(parsed)
            entry["interesting_keys"] = sorted(interesting)[:40]
            entry["carries_market_data"] = bool(interesting)
            entry["preview"] = redact_structure(parsed)
        except Exception:
            entry["parse_failed"] = True
        findings.append(entry)

    try:
        blobs = await page.evaluate(
            """() => Array.from(
                document.querySelectorAll('script[type="application/json"]')
            ).map(s => ({id: s.id || null, text: s.textContent || ''}))"""
        )
    except Exception:
        blobs = []
    for index, blob in enumerate(blobs or []):
        text = blob.get("text") or ""
        entry = {
            "source": f'script[type="application/json"]#{blob.get("id") or index}',
            "size_bytes": len(text),
        }
        try:
            parsed = json.loads(text)
            entry["top_level_keys"] = (
                list(parsed)[:40] if isinstance(parsed, dict) else f"<list:{len(parsed)}>"
            )
            interesting = _find_interesting_keys(parsed)
            entry["interesting_keys"] = sorted(interesting)[:40]
            entry["carries_market_data"] = bool(interesting)
            entry["preview"] = redact_structure(parsed)
        except Exception:
            entry["parse_failed"] = True
        findings.append(entry)

    # RSC streaming payload. Not JSON as a whole, so it is probed for
    # price-shaped markers textually rather than parsed.
    try:
        rsc_size = await page.evaluate(
            """() => (self.__next_f || [])
                .map(c => (Array.isArray(c) && typeof c[1] === 'string') ? c[1] : '')
                .join('').length"""
        )
        if rsc_size:
            rsc_markers = await page.evaluate(
                """(markers) => {
                    const text = (self.__next_f || [])
                        .map(c => (Array.isArray(c) && typeof c[1] === 'string') ? c[1] : '')
                        .join('').toLowerCase();
                    return markers.filter(m => text.includes(m));
                }""",
                list(_INTERESTING_KEY_MARKERS),
            )
            findings.append(
                {
                    "source": "self.__next_f (React Server Component payload)",
                    "size_bytes": rsc_size,
                    "interesting_keys": rsc_markers,
                    "carries_market_data": bool(rsc_markers),
                    "note": "RSC stream is not whole-JSON; probed textually for price markers.",
                }
            )
    except Exception:
        pass

    return findings


def classify(discovery: NetworkDiscovery) -> dict[str, Any]:
    """Decide A/B/C/D from what was observed."""
    json_endpoints = [
        r for r in discovery.responses
        if r.get("is_json")
        and r.get("carries_market_data")
        and r.get("status") == 200
        and r.get("resource_type") != "document"
    ]
    graphql = [r for r in json_endpoints if r.get("is_graphql") or "graphql" in r["url"].lower()]
    next_data_routes = [r for r in json_endpoints if "/_next/data/" in r["url"]]
    embedded = [e for e in discovery.embedded if e.get("carries_market_data")]

    if graphql:
        verdict = "B"
        summary = "GraphQL endpoint carries the market data."
        targets = [r["url"] for r in graphql][:5]
    elif json_endpoints:
        verdict = "A"
        summary = "A reusable JSON/XHR endpoint carries the market data."
        targets = [r["url"] for r in (next_data_routes or json_endpoints)][:5]
    elif embedded:
        verdict = "C"
        summary = "Market data is embedded in the delivered HTML (no separate request)."
        targets = [e["source"] for e in embedded][:5]
    else:
        verdict = "D"
        summary = (
            "No reusable JSON payload observed - data appears to exist only in "
            "rendered HTML. Browser rendering may be unavoidable."
        )
        targets = []

    return {
        "verdict": verdict,
        "summary": summary,
        "candidate_targets": targets,
        "json_endpoints_seen": len(json_endpoints),
        "graphql_endpoints_seen": len(graphql),
        "next_data_routes_seen": len(next_data_routes),
        "embedded_sources_with_market_data": len(embedded),
    }


async def run_discovery(
    url: str,
    *,
    headless: bool = True,
    user_agent: str | None = None,
    timeout_ms: int = 45000,
    price_selector: str = "#prices-overview",
    settle_ms: int = 4000,
    report_path: str | None = None,
) -> dict[str, Any]:
    """Load one player page with listeners attached and classify the source."""
    log.info("discovery: starting for url=%s", url)
    discovery = NetworkDiscovery(url)
    # Bound up-front: a navigation failure must still produce a report
    # (the captured responses are often exactly what explains the failure)
    # rather than dying with a NameError on the way out.
    html_size = 0
    price_attached = False
    dom_before_scroll: dict[str, Any] = {}
    dom_after_scroll: dict[str, Any] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        try:
            context = await browser.new_context(
                user_agent=user_agent,
                locale="en-GB",
                viewport={"width": 1440, "height": 1000},
            )
            page = await context.new_page()

            # Attached before navigation so the document response and every
            # hydration-time request are captured.
            discovery.attach(page)

            response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            status = response.status if response is not None else 0
            log.info("discovery: navigation status=%s", status)

            price_attached = False
            try:
                await page.locator(price_selector).wait_for(state="attached", timeout=timeout_ms)
                price_attached = True
                log.info("discovery: %s attached", price_selector)
            except Exception:
                log.warning("discovery: %s never attached", price_selector)

            # Let post-hydration requests fire - the whole point is to see
            # what happens AFTER the document arrives.
            await page.wait_for_timeout(settle_ms)

            dom_before_scroll = await inspect_dom_candidates(page, price_selector)

            # Scroll and re-check. If the container only appears after this,
            # the cause is a lazy/viewport-triggered mount rather than a
            # markup rename - a completely different fix, and one the
            # production worker (which never scrolls) would never trigger.
            dom_after_scroll: dict[str, Any] = {}
            if not price_attached:
                try:
                    await page.evaluate(
                        "() => window.scrollTo(0, document.body.scrollHeight)"
                    )
                    await page.wait_for_timeout(2500)
                    dom_after_scroll = await inspect_dom_candidates(page, price_selector)
                except Exception:
                    log.exception("discovery: scroll probe failed")

            discovery.embedded = await inspect_embedded_json(page)
            await discovery.drain()

            html_size = len(await page.content())
        finally:
            await browser.close()

    verdict = classify(discovery)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "rendered_html_bytes": html_size,
        "classification": verdict,
        "price_selector": price_selector,
        "price_selector_attached": price_attached,
        "dom_before_scroll": dom_before_scroll,
        "dom_after_scroll": dom_after_scroll,
        "embedded_sources": discovery.embedded,
        "responses": discovery.responses,
    }

    _log_report(report)

    if report_path:
        try:
            with open(report_path, "w", encoding="utf-8") as handle:
                json.dump(report, handle, indent=2, default=str)
            log.info("discovery: report written to %s", report_path)
        except Exception:
            log.exception("discovery: could not write report to %s", report_path)

    return report


def _log_dom_findings(report: dict[str, Any]) -> None:
    """Report what the price container is actually called.

    This is the section that resolves a `price_section_missing` outage:
    either the expected selector is simply gone (markup rename - adopt one
    of the candidates below), or it appears only after scrolling (lazy
    mount - the worker never scrolls, so it would never see it), or the
    page is a bot challenge (challenge_markers non-empty).
    """
    before = report.get("dom_before_scroll") or {}
    after = report.get("dom_after_scroll") or {}
    if not before:
        return

    log.info(
        "DOM: selector=%s attached=%s title=%r fc_card_count=%s body_text=%s chars "
        "scroll_height=%s viewport=%s",
        report.get("price_selector"),
        report.get("price_selector_attached"),
        before.get("title"),
        before.get("fc_card_count"),
        before.get("body_text_length"),
        before.get("scroll_height"),
        before.get("viewport_height"),
    )

    if before.get("challenge_markers"):
        log.warning(
            "DOM: BOT-CHALLENGE MARKERS PRESENT %s - the page served is not the "
            "player page; treat as a block, not a markup change.",
            before["challenge_markers"],
        )

    candidates = before.get("candidates") or []
    log.info("DOM: %d price-shaped candidate elements before scroll", len(candidates))
    for candidate in candidates[:30]:
        log.info(
            "   <%s id=%s class=%s testid=%s> digits=%s len=%s text=%r",
            candidate.get("tag"), candidate.get("id"), candidate.get("class"),
            candidate.get("testid"), candidate.get("has_digits"),
            candidate.get("text_length"), candidate.get("text_preview"),
        )

    if after:
        appeared = after.get("price_selector_present") and not before.get(
            "price_selector_present"
        )
        log.info(
            "DOM after scroll: selector_present=%s candidates=%d",
            after.get("price_selector_present"), len(after.get("candidates") or []),
        )
        if appeared:
            log.warning(
                "DOM: selector appeared ONLY AFTER SCROLLING - this is a lazy/"
                "viewport-triggered mount, not a markup rename. The worker never "
                "scrolls, which is why every card reports price_section_missing."
            )


def _log_report(report: dict[str, Any]) -> None:
    verdict = report["classification"]
    log.info("=" * 72)
    log.info("FUT.GG PRICE DATA DISCOVERY")
    log.info("url=%s rendered_html=%.0f KB", report["url"], report["rendered_html_bytes"] / 1024)
    log.info("VERDICT: %s - %s", verdict["verdict"], verdict["summary"])
    for target in verdict["candidate_targets"]:
        log.info("  candidate: %s", target)
    log.info("-" * 72)

    interesting = [
        r for r in report["responses"]
        if r.get("carries_market_data") or r.get("is_json")
    ]
    log.info("responses captured=%d json/market-bearing=%d",
             len(report["responses"]), len(interesting))

    for entry in interesting[:25]:
        log.info(
            "  %s %s status=%s type=%s ct=%s body=%dB json=%s market_data=%s",
            entry.get("method"), entry.get("url"), entry.get("status"),
            entry.get("resource_type"), entry.get("response_content_type"),
            entry.get("body_size_bytes", 0), entry.get("is_json"),
            entry.get("carries_market_data", False),
        )
        if entry.get("top_level_keys"):
            log.info("      top_level_keys=%s", entry["top_level_keys"])
        if entry.get("interesting_keys"):
            log.info("      market_keys=%s", entry["interesting_keys"])
        if entry.get("request_headers"):
            log.info("      request_headers=%s", entry["request_headers"])
        if entry.get("post_body_preview"):
            log.info("      post_body=%s", entry["post_body_preview"])
        if entry.get("preview"):
            log.info("      preview=%s", json.dumps(entry["preview"], default=str)[:1200])

    log.info("-" * 72)
    _log_dom_findings(report)

    log.info("-" * 72)
    for embedded in report["embedded_sources"]:
        log.info(
            "  embedded %s size=%dB market_data=%s",
            embedded.get("source"), embedded.get("size_bytes", 0),
            embedded.get("carries_market_data", False),
        )
        if embedded.get("interesting_keys"):
            log.info("      market_keys=%s", embedded["interesting_keys"])
        if embedded.get("top_level_keys"):
            log.info("      top_level_keys=%s", embedded["top_level_keys"])
    log.info("=" * 72)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    target_url = os.getenv("FUTGG_DISCOVER_URL", "").strip()
    if not target_url:
        raise SystemExit("Set FUTGG_DISCOVER_URL to a full FUT.GG player URL")
    asyncio.run(
        run_discovery(
            target_url,
            headless=os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() != "false",
            report_path=os.getenv("FUTGG_DISCOVER_REPORT_PATH") or None,
        )
    )
