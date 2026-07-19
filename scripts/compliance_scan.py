"""Refuses a file that is not safe to publish.

The hackathon submission cannot be edited after it is submitted, so anything
wrong at submit time is wrong permanently: a workspace path pasted into a code
block, an unrelated private project name left in a sentence, a link that 404s
for a judge who has no way to ask a question. This reads one file and fails if
it finds any of those, which is why it takes a path argument — the same gate
runs against the draft while it is being written and against the rendered
final packet before submit.

Two biases, both deliberate and both one-directional:

* False positives are cheap here and false negatives are not. A flagged line
  costs a few seconds to dismiss; a leaked one is public and cannot be
  withdrawn.
* Nothing is skipped quietly. A check that cannot run says so on stderr, and
  ``--require-terms`` turns that into a hard failure for the pre-submit run.
  A gate that silently passes is worse than no gate, because it is believed.

The private terms deliberately do not live in this file. This repository is
public, so hardcoding the names this script exists to keep out of the packet
would publish them itself. Supply them with ``--terms``, via the
``FORESHOCK_COMPLIANCE_TERMS`` environment variable, or in a gitignored
``compliance-terms.local.txt`` beside the repository root: one term per line,
blank lines and ``#`` comments ignored. Matching is case-insensitive and
whitespace-flexible, so multi-word names survive being wrapped across lines.

Usage:
    python scripts/compliance_scan.py docs/SUBMISSION.md
    python scripts/compliance_scan.py --require-terms docs/SUBMISSION.md
    python scripts/compliance_scan.py --offline docs/SUBMISSION.md
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_USAGE = 2

DEFAULT_TERMS_FILENAME = "compliance-terms.local.txt"
TERMS_ENV_VAR = "FORESHOCK_COMPLIANCE_TERMS"
URL_TIMEOUT_SECONDS = 10.0

# A browser User-Agent, because several hosts answer a bare urllib agent with
# 403. Reporting a link as dead when it opens fine for a judge is the kind of
# false alarm that teaches an operator to skim past this output.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Correct in a quickstart, meaningless to resolve from here.
LOCAL_HOSTNAMES = frozenset({"localhost", "127.0.0.1", "0.0.0.0", "::1"})

WORKSPACE_PATH_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("windows user directory", re.compile(r"[A-Za-z]:\\Users\\", re.IGNORECASE)),
    ("posix home directory", re.compile(r"/(?:home|Users)/[A-Za-z0-9._-]+/")),
    ("agent harness directory", re.compile(r"[~/\\]?\.claude(?![\w-])")),
    ("local workspace tree", re.compile(r"\bDesktop[/\\]", re.IGNORECASE)),
    ("UNC network path", re.compile(r"\\\\[A-Za-z0-9._-]+\\")),
)

URL_PATTERN = re.compile(r"https?://[^\s<>()\[\]\"'`]+", re.IGNORECASE)
TRAILING_PUNCTUATION = ".,;:!?"

#: Returns None when a URL resolves, otherwise a short reason it did not.
UrlResolver = Callable[[str], "str | None"]


@dataclass(frozen=True)
class Finding:
    """One reason the file is not safe to submit."""

    kind: str
    line_number: int
    detail: str
    line: str

    def render(self) -> str:
        return f"  {self.kind:<14} line {self.line_number:<4} {self.detail}\n" f"      | {self.line.strip()}"


@dataclass(frozen=True)
class ScanResult:
    """What the gate saw, including what it could not check."""

    path: Path
    findings: tuple[Finding, ...]
    terms_source: Path | None
    urls_checked: int
    notices: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.findings


def parse_terms(raw: str) -> tuple[str, ...]:
    """Read a term list. Only a leading ``#`` comments a line out, so a term
    containing one is still usable."""
    terms = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            terms.append(stripped)
    return tuple(terms)


def resolve_terms_path(explicit: str | None, *, repo_root: Path) -> Path | None:
    """Locate the term list, preferring the most explicit source.

    An explicitly named file that does not exist is an error rather than a
    fallback: the operator asked for that list specifically, and quietly
    scanning without it would report a pass the run did not earn.
    """
    if explicit is not None:
        candidate = Path(explicit)
        if not candidate.is_file():
            raise FileNotFoundError(f"term list not found: {candidate}")
        return candidate

    from_env = os.environ.get(TERMS_ENV_VAR)
    if from_env:
        candidate = Path(from_env)
        if not candidate.is_file():
            raise FileNotFoundError(
                f"{TERMS_ENV_VAR} points at a missing file: {candidate}"
            )
        return candidate

    default = repo_root / DEFAULT_TERMS_FILENAME
    return default if default.is_file() else None


def compile_terms(terms: Sequence[str]) -> re.Pattern[str] | None:
    """Build one matcher for every term.

    Longest first so an overlapping shorter term cannot claim the match and
    report a narrower name than the one actually present. Lookarounds rather
    than ``\\b`` because a term may begin or end with punctuation, as an email
    address does, and ``\\b`` would not anchor it.
    """
    if not terms:
        return None
    alternatives = [
        r"\s+".join(re.escape(word) for word in term.split())
        for term in sorted(terms, key=len, reverse=True)
    ]
    return re.compile(rf"(?<!\w)(?:{'|'.join(alternatives)})(?!\w)", re.IGNORECASE)


def scan_text(text: str, term_matcher: re.Pattern[str] | None) -> list[Finding]:
    """Find private terms and workspace paths. Pure, so the rules are testable
    without touching a filesystem or a network.

    Terms are matched against the whole text rather than line by line, because
    hard-wrapped Markdown splits a two-word name across a newline and a
    per-line scan would walk straight past it. Paths cannot wrap, so those stay
    per-line and keep their exact matched text.
    """
    findings: list[Finding] = []
    lines = text.splitlines()

    if term_matcher is not None:
        for match in term_matcher.finditer(text):
            number = text.count("\n", 0, match.start()) + 1
            line = lines[number - 1] if number <= len(lines) else ""
            matched = " ".join(match.group(0).split())
            findings.append(
                Finding("private-term", number, f"private term {matched!r}", line)
            )

    for number, line in enumerate(lines, start=1):
        for label, pattern in WORKSPACE_PATH_RULES:
            for match in pattern.finditer(line):
                findings.append(
                    Finding("workspace-path", number, f"{label} {match.group(0)!r}", line)
                )
    return findings


def extract_urls(text: str) -> list[tuple[int, str, str]]:
    """Pull every http(s) URL out as ``(line number, url, line)``.

    Sentence punctuation is stripped from the end because a URL that ends a
    sentence in Markdown would otherwise be reported dead for the trailing
    period rather than for anything wrong with the link.
    """
    found: list[tuple[int, str, str]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        for match in URL_PATTERN.finditer(line):
            found.append((number, match.group(0).rstrip(TRAILING_PUNCTUATION), line))
    return found


def is_local_url(url: str) -> bool:
    host = urllib.parse.urlsplit(url).hostname or ""
    return host.lower() in LOCAL_HOSTNAMES


def resolve_url(url: str, timeout: float = URL_TIMEOUT_SECONDS) -> str | None:
    """Return None when the URL resolves, else a short reason it did not.

    HEAD first because it is cheap, then GET on failure: plenty of hosts answer
    HEAD with 405 or 403 while serving the same URL correctly to a browser.
    """
    reason = "unreachable"
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(
            url, method=method, headers={"User-Agent": USER_AGENT}
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                if response.status < 400:
                    return None
                reason = f"HTTP {response.status}"
        except urllib.error.HTTPError as error:
            reason = f"HTTP {error.code}"
        except urllib.error.URLError as error:
            reason = f"unreachable ({error.reason})"
        except (TimeoutError, OSError) as error:  # pragma: no cover - transport edge
            reason = f"unreachable ({error})"
    return reason


def check_urls(text: str, resolve: UrlResolver) -> tuple[list[Finding], int]:
    """Verify each remote URL resolves, returning findings and the count checked.

    Local hosts are skipped by design: they are correct in a quickstart and
    resolving them from here proves nothing. Each distinct URL is resolved once
    however many times it appears.
    """
    findings: list[Finding] = []
    seen: dict[str, str | None] = {}
    for number, url, line in extract_urls(text):
        if is_local_url(url):
            continue
        if url not in seen:
            seen[url] = resolve(url)
        reason = seen[url]
        if reason is not None:
            findings.append(Finding("dead-url", number, f"{url} -> {reason}", line))
    return findings, len(seen)


def scan_file(
    path: Path,
    *,
    terms: Sequence[str] = (),
    terms_source: Path | None = None,
    resolve: UrlResolver | None = None,
) -> ScanResult:
    """Run every check that can run, and record every check that cannot."""
    text = path.read_text(encoding="utf-8")
    notices: list[str] = []

    if terms_source is None:
        notices.append(
            "no private-term list found; scanned for workspace paths and dead "
            "links only (see --terms)"
        )

    findings = scan_text(text, compile_terms(terms))

    if resolve is None:
        notices.append("offline: URLs were extracted but not resolved")
        urls_checked = 0
    else:
        url_findings, urls_checked = check_urls(text, resolve)
        findings.extend(url_findings)

    findings.sort(key=lambda finding: (finding.line_number, finding.kind))
    return ScanResult(
        path=path,
        findings=tuple(findings),
        terms_source=terms_source,
        urls_checked=urls_checked,
        notices=tuple(notices),
    )


def render_report(result: ScanResult) -> str:
    lines = [f"compliance scan: {result.path}"]
    source = result.terms_source or "none"
    lines.append(f"  term list      : {source}")
    lines.append(f"  urls resolved  : {result.urls_checked}")
    for notice in result.notices:
        lines.append(f"  NOTICE         : {notice}")
    if result.ok:
        lines.append("  RESULT         : clean")
    else:
        lines.append(f"  RESULT         : {len(result.findings)} finding(s)")
        lines.extend(finding.render() for finding in result.findings)
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refuse a file that is not safe to publish.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("path", help="File to scan (the draft, or the final packet).")
    parser.add_argument(
        "--terms",
        default=None,
        help=f"Private-term list. Defaults to {TERMS_ENV_VAR} or {DEFAULT_TERMS_FILENAME}.",
    )
    parser.add_argument(
        "--require-terms",
        action="store_true",
        help="Fail if no term list is found. Use this for the pre-submit run.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip URL resolution. Never use this for the pre-submit run.",
    )
    return parser


def main(argv: Sequence[str] | None = None, *, repo_root: Path | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = repo_root or Path(__file__).resolve().parent.parent

    target = Path(args.path)
    if not target.is_file():
        print(f"error: no such file: {target}", file=sys.stderr)
        return EXIT_USAGE

    try:
        terms_source = resolve_terms_path(args.terms, repo_root=repo_root)
    except FileNotFoundError as error:
        print(f"error: {error}", file=sys.stderr)
        return EXIT_USAGE

    if terms_source is None and args.require_terms:
        print(
            "error: --require-terms was given but no private-term list was found",
            file=sys.stderr,
        )
        return EXIT_USAGE

    terms = parse_terms(terms_source.read_text(encoding="utf-8")) if terms_source else ()
    result = scan_file(
        target,
        terms=terms,
        terms_source=terms_source,
        resolve=None if args.offline else resolve_url,
    )

    print(render_report(result))
    for notice in result.notices:
        print(f"warning: {notice}", file=sys.stderr)
    return EXIT_OK if result.ok else EXIT_FINDINGS


if __name__ == "__main__":
    sys.exit(main())
