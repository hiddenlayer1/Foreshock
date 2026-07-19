"""Tests for the pre-submit compliance gate.

The gate's entire job is to refuse a file, so the tests that carry the weight
are the ones proving it actually refuses: a gate nobody has watched reject
something is indistinguishable from a gate that always passes.

Every term used here is invented. Putting the real ones in a test file inside
a public repository would leak exactly what the scanner exists to catch.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

# Loaded by path because scripts/ is not an importable package. The module has
# to be registered before it executes: its dataclasses use string annotations,
# and dataclasses resolves those through sys.modules[cls.__module__].
_MODULE_PATH = Path(__file__).resolve().parent.parent / "scripts" / "compliance_scan.py"
_SPEC = importlib.util.spec_from_file_location("compliance_scan", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
scanner = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = scanner
_SPEC.loader.exec_module(scanner)

TERMS = ("Nightjar", "Acme Internal", "someone@example-private.test")


def _matcher(*terms: str) -> Any:
    return scanner.compile_terms(terms or TERMS)


def _kinds(findings: Any) -> list[str]:
    return [finding.kind for finding in findings]


def _always_ok(url: str) -> None:
    return None


def _always_dead(url: str) -> str:
    return "HTTP 404"


# --- private terms ---------------------------------------------------------


def test_clean_text_produces_no_findings() -> None:
    findings = scanner.scan_text("Foreshock watches DataHub's change stream.", _matcher())
    assert findings == []


def test_private_term_is_flagged() -> None:
    """The check the whole gate exists for."""
    findings = scanner.scan_text("Built on top of Nightjar internally.", _matcher())
    assert _kinds(findings) == ["private-term"]
    assert "Nightjar" in findings[0].detail
    assert findings[0].line_number == 1


def test_private_term_match_is_case_insensitive() -> None:
    findings = scanner.scan_text("we reused NIGHTJAR here", _matcher())
    assert _kinds(findings) == ["private-term"]


def test_multi_word_term_is_caught_when_markdown_wraps_it() -> None:
    """Hard-wrapped prose splits a two-word name across a newline; a per-line
    scan would walk straight past it."""
    findings = scanner.scan_text("credit goes to the Acme\nInternal team", _matcher())
    assert _kinds(findings) == ["private-term"]
    assert findings[0].detail.endswith("'Acme Internal'")


def test_term_does_not_match_inside_a_longer_word() -> None:
    """Substring matching would flag ordinary prose and train the operator to
    skim past real findings."""
    assert scanner.scan_text("the nightjarring effect", _matcher()) == []


def test_term_containing_punctuation_is_matched() -> None:
    findings = scanner.scan_text("ping someone@example-private.test for access", _matcher())
    assert _kinds(findings) == ["private-term"]


def test_longest_term_wins_an_overlap() -> None:
    """Reporting the shorter name would understate what is actually exposed."""
    findings = scanner.scan_text("the Acme Internal roadmap", _matcher("Acme", "Acme Internal"))
    assert findings[0].detail.endswith("'Acme Internal'")


def test_no_term_list_means_no_term_findings() -> None:
    assert scanner.scan_text("Nightjar everywhere", scanner.compile_terms(())) == []


# --- workspace paths -------------------------------------------------------


def test_windows_workspace_path_is_flagged() -> None:
    findings = scanner.scan_text(r"run C:\Users\someone\Desktop\thing", _matcher())
    assert "workspace-path" in _kinds(findings)


def test_posix_home_path_is_flagged() -> None:
    findings = scanner.scan_text("see /home/someone/notes.md", _matcher())
    assert _kinds(findings) == ["workspace-path"]


def test_agent_harness_directory_is_flagged() -> None:
    findings = scanner.scan_text("cached under ~/.claude/plans", _matcher())
    assert _kinds(findings) == ["workspace-path"]


def test_unc_path_is_flagged() -> None:
    findings = scanner.scan_text(r"copied from \\fileserver\share", _matcher())
    assert _kinds(findings) == ["workspace-path"]


def test_ordinary_relative_path_is_not_flagged() -> None:
    assert scanner.scan_text("see docs/running-datahub-on-podman.md", _matcher()) == []


def test_findings_are_reported_in_line_order() -> None:
    text = "clean line\nNightjar here\nclean\n/home/someone/x\n"
    numbers = [finding.line_number for finding in scanner.scan_text(text, _matcher())]
    assert numbers == sorted(numbers)


# --- urls ------------------------------------------------------------------


def test_live_url_is_not_flagged() -> None:
    findings, checked = scanner.check_urls("see https://example.com/a", _always_ok)
    assert findings == []
    assert checked == 1


def test_dead_url_is_flagged() -> None:
    findings, _ = scanner.check_urls("see https://example.com/gone", _always_dead)
    assert _kinds(findings) == ["dead-url"]
    assert "HTTP 404" in findings[0].detail


def test_localhost_url_is_never_resolved() -> None:
    """Local hosts are correct in a quickstart, and resolving them from here
    proves nothing about what a judge would see."""
    calls: list[str] = []

    def record(url: str) -> None:
        calls.append(url)
        return None

    findings, checked = scanner.check_urls(
        "start at http://localhost:9002 and http://127.0.0.1:8080", record
    )
    assert calls == []
    assert findings == []
    assert checked == 0


def test_each_distinct_url_is_resolved_once() -> None:
    calls: list[str] = []

    def record(url: str) -> None:
        calls.append(url)
        return None

    scanner.check_urls(
        "https://example.com/a then https://example.com/a then https://example.com/b",
        record,
    )
    assert sorted(calls) == ["https://example.com/a", "https://example.com/b"]


def test_trailing_sentence_punctuation_is_not_part_of_the_url() -> None:
    """Otherwise a link that ends a sentence is reported dead for the period."""
    urls = [url for _, url, _ in scanner.extract_urls("Read https://example.com/a.")]
    assert urls == ["https://example.com/a"]


def test_markdown_link_target_is_extracted_without_the_bracket() -> None:
    urls = [url for _, url, _ in scanner.extract_urls("[repo](https://example.com/r) ok")]
    assert urls == ["https://example.com/r"]


# --- term list loading -----------------------------------------------------


def test_terms_file_ignores_comments_and_blank_lines() -> None:
    parsed = scanner.parse_terms("# a comment\n\nNightjar\n  Acme Internal  \n\n")
    assert parsed == ("Nightjar", "Acme Internal")


def test_terms_file_keeps_a_term_containing_a_hash() -> None:
    assert scanner.parse_terms("issue#42-codename") == ("issue#42-codename",)


def test_default_term_list_is_found_in_the_repo_root(tmp_path: Path) -> None:
    (tmp_path / scanner.DEFAULT_TERMS_FILENAME).write_text("Nightjar\n", encoding="utf-8")
    assert scanner.resolve_terms_path(None, repo_root=tmp_path) is not None


def test_absent_term_list_resolves_to_none(tmp_path: Path) -> None:
    assert scanner.resolve_terms_path(None, repo_root=tmp_path) is None


def test_explicitly_named_missing_term_list_is_an_error(tmp_path: Path) -> None:
    """Falling back silently would report a pass the run did not earn."""
    try:
        scanner.resolve_terms_path(str(tmp_path / "nope.txt"), repo_root=tmp_path)
    except FileNotFoundError:
        return
    raise AssertionError("a missing --terms file must not fall back silently")


# --- reporting and exit codes ----------------------------------------------


def test_scan_file_reports_a_missing_term_list_rather_than_passing_quietly(
    tmp_path: Path,
) -> None:
    target = tmp_path / "packet.md"
    target.write_text("all clean here\n", encoding="utf-8")
    result = scanner.scan_file(target, terms=(), terms_source=None, resolve=None)
    assert result.ok
    assert any("no private-term list" in notice for notice in result.notices)


def test_offline_scan_records_that_urls_went_unchecked(tmp_path: Path) -> None:
    target = tmp_path / "packet.md"
    target.write_text("see https://example.com/a\n", encoding="utf-8")
    result = scanner.scan_file(target, terms=TERMS, terms_source=target, resolve=None)
    assert any("offline" in notice for notice in result.notices)
    assert result.urls_checked == 0


def test_main_exits_non_zero_when_a_private_term_is_present(tmp_path: Path) -> None:
    """§8 check 1: the gate must be shown to fail on a private term."""
    terms = tmp_path / "terms.txt"
    terms.write_text("Nightjar\n", encoding="utf-8")
    target = tmp_path / "packet.md"
    target.write_text("Built with Nightjar.\n", encoding="utf-8")

    code = scanner.main(
        [str(target), "--offline", "--terms", str(terms)], repo_root=tmp_path
    )
    assert code == scanner.EXIT_FINDINGS


def test_main_exits_non_zero_when_a_workspace_path_is_present(tmp_path: Path) -> None:
    """§8 check 1: and on a workspace path, with no term list involved."""
    target = tmp_path / "packet.md"
    target.write_text("clone it into C:\\Users\\someone\\Desktop\\Code\n", encoding="utf-8")

    code = scanner.main([str(target), "--offline"], repo_root=tmp_path)
    assert code == scanner.EXIT_FINDINGS


def test_main_exits_zero_on_a_clean_file(tmp_path: Path) -> None:
    target = tmp_path / "packet.md"
    target.write_text("Foreshock reads DataHub's change stream.\n", encoding="utf-8")

    code = scanner.main([str(target), "--offline"], repo_root=tmp_path)
    assert code == scanner.EXIT_OK


def test_main_rejects_a_missing_target(tmp_path: Path) -> None:
    code = scanner.main([str(tmp_path / "absent.md"), "--offline"], repo_root=tmp_path)
    assert code == scanner.EXIT_USAGE


def test_require_terms_fails_when_no_list_is_found(tmp_path: Path) -> None:
    """The pre-submit run must not be able to pass while the term check is off."""
    target = tmp_path / "packet.md"
    target.write_text("all clean\n", encoding="utf-8")

    code = scanner.main(
        [str(target), "--offline", "--require-terms"], repo_root=tmp_path
    )
    assert code == scanner.EXIT_USAGE


def test_report_names_every_finding(tmp_path: Path) -> None:
    target = tmp_path / "packet.md"
    target.write_text("Nightjar lives in /home/someone/x\n", encoding="utf-8")
    result = scanner.scan_file(target, terms=TERMS, terms_source=target, resolve=None)
    report = scanner.render_report(result)
    assert "2 finding(s)" in report
    assert "private-term" in report
    assert "workspace-path" in report
