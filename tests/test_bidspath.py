"""Tests for the shared registry path builder (pipeio.bidspath).

Expected strings were produced by ``snakebids.paths.bids_factory`` (snakebids
0.15) for each spec; ``test_matches_snakebids`` re-checks when snakebids is
installed.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import yaml

from pipeio.bidspath import (
    align_entities,
    bids_path,
    config_base_wildcards,
    detect_bids_spec,
    family_kwargs,
)

CASES = [
    dict(root="out/spectrogram/bursts", datatype="ieeg", suffix="bursts",
         extension=".tsv", subject="03", session="24", task="pre"),
    dict(root="out/x", datatype="ecephys", suffix="ev", extension=".tsv",
         subject="03", session="24", task="pre", acquisition="lshank", recording="lf"),
    dict(root="out/x", suffix="ev", extension=".npz", subject="{subject}",
         desc="clean", band="spindle", run="1"),
    dict(root="out", suffix="done", extension=".flag"),
    dict(root="out/y", datatype="ieeg", suffix="ieeg", extension=".lfp",
         sub="01", ses="02", acq="a", hemi="L", space="T1w"),
]

EXPECTED = {
    "v0_0_0": [
        "out/spectrogram/bursts/sub-03/ses-24/ieeg/sub-03_ses-24_task-pre_bursts.tsv",
        "out/x/sub-03/ses-24/ecephys/sub-03_ses-24_task-pre_acquisition-lshank_recording-lf_ev.tsv",
        "out/x/sub-{subject}/sub-{subject}_run-1_desc-clean_band-spindle_ev.npz",
        "out/done.flag",
        "out/y/sub-01/ses-02/ieeg/sub-01_ses-02_acq-a_hemi-L_space-T1w_ieeg.lfp",
    ],
    "v0_15_0": [
        "out/spectrogram/bursts/sub-03/ses-24/ieeg/sub-03_ses-24_task-pre_bursts.tsv",
        "out/x/sub-03/ses-24/ecephys/sub-03_ses-24_task-pre_acq-lshank_recording-lf_ev.tsv",
        "out/x/sub-{subject}/sub-{subject}_run-1_band-spindle_desc-clean_ev.npz",
        "out/done.flag",
        "out/y/sub-01/ses-02/ieeg/sub-01_ses-02_acq-a_space-T1w_hemi-L_ieeg.lfp",
    ],
}


@pytest.mark.parametrize("spec", sorted(EXPECTED))
def test_bids_path_layout(spec):
    assert [bids_path(spec=spec, **c) for c in CASES] == EXPECTED[spec]
    assert [bids_path(spec="latest", **c) for c in CASES] == EXPECTED["v0_15_0"]


@pytest.mark.parametrize("spec", ["v0_0_0", "v0_11_0", "v0_15_0"])
def test_matches_snakebids(spec):
    try:
        from snakebids.paths import bids_factory, specs
    except (ImportError, AttributeError):  # absent, or mocked by other tests
        pytest.skip("snakebids not installed")
    build = bids_factory(getattr(specs, spec)())
    for c in CASES:
        assert bids_path(spec=spec, **c) == build(**c)


def test_long_and_short_name_collide():
    with pytest.raises(ValueError):
        bids_path(spec="latest", root="x", suffix="s", subject="1", sub="1")


def test_family_kwargs_precedence():
    registry = {
        "fam": {
            "base_input": "ieeg",
            "bids": {"root": "r", "datatype": "ieeg", "desc": "fam"},
            "members": {"m": {"suffix": "s", "extension": ".tsv", "desc": "mem"}},
        }
    }
    kw = family_kwargs(
        registry, "fam", "m", {"task": "pre"},
        base_wildcards={"ieeg": {"subject": "{subject}", "task": "{task}"}},
    )
    assert kw == {
        "subject": "{subject}", "task": "pre", "root": "r", "datatype": "ieeg",
        "desc": "mem", "suffix": "s", "extension": ".tsv",
    }


def test_family_kwargs_errors():
    registry = {"fam": {"base_input": "ieeg", "members": {"m": {"suffix": "s"}}}}
    with pytest.raises(KeyError, match="Unknown family 'nope'"):
        family_kwargs(registry, "nope", "m", base_wildcards={})
    with pytest.raises(KeyError, match="Unknown member 'x'"):
        family_kwargs(registry, "fam", "x", base_wildcards={"ieeg": {}})
    with pytest.raises(ValueError, match="no inputs were provided"):
        family_kwargs(registry, "fam", "m")


def test_detect_bids_spec():
    assert detect_bids_spec('set_bids_spec("v0_0_0")\n') == "v0_0_0"
    assert detect_bids_spec("from snakebids import bids\n") == "latest"
    assert detect_bids_spec("", "set_bids_spec('v0_11_0')") == "v0_11_0"


def test_config_base_wildcards():
    cfg = {
        "pybids_inputs": {"ieeg": {"wildcards": ["subject", "session"]}},
        "pybids_inputs_meta": {"meta_json": {"wildcards": ["subject"]}},
        "other": {"x": {"wildcards": ["nope"]}},
    }
    assert config_base_wildcards(cfg) == {
        "ieeg": {"subject": "{subject}", "session": "{session}"},
        "meta_json": {"subject": "{subject}"},
    }


def test_align_entities():
    known = {"subject": "{subject}", "acquisition": "{acquisition}"}
    out = align_entities({"sub": "01", "acq": "a", "band": "x"}, known, "v0_0_0")
    assert out == {"subject": "01", "acquisition": "a", "band": "x"}


# ---------------------------------------------------------------------------
# SimpleResolver / pipeio_target_paths on a spectrogram_burst-shaped flow
# ---------------------------------------------------------------------------

_SNAKEFILE = '''\
from snakebids import generate_inputs, set_bids_spec
set_bids_spec("v0_0_0")
from pipeio.adapters.bids import BidsPaths
configfile: "config.yml"
_registry = dict(config.get("registry") or {})
for _band in ["spindle"]:
    _registry[f"bursts_{_band}"] = {
        "base_input": "ieeg",
        "bids": {"root": f"spectrogram/bursts_{_band}", "datatype": "ieeg"},
        "members": {"bursts": {"suffix": f"bursts_{_band}", "extension": ".tsv"}},
    }
out_paths = BidsPaths(_registry, config["output_dir"], inputs)
'''

_CONFIG = {
    "input_dir": "raw",
    "output_dir": "derivatives/spectrogram_burst",
    "pybids_inputs": {
        "ieeg": {"filters": {"suffix": "ieeg"}, "wildcards": ["subject", "session", "task"]},
        "ecephys": {"filters": {"suffix": "ecephys"},
                    "wildcards": ["subject", "session", "task", "acquisition"]},
    },
    "registry": {
        "bursts": {
            "base_input": "ieeg",
            "bids": {"root": "spectrogram/bursts", "datatype": "ieeg"},
            "members": {"bursts": {"suffix": "bursts", "extension": ".tsv"}},
        },
        "npx": {
            "base_input": "ecephys",
            "bids": {"root": "detect/npx", "datatype": "ecephys"},
            "members": {"events": {"suffix": "ev", "extension": ".tsv", "desc": "raw"}},
        },
    },
}

BURSTS = (
    "derivatives/spectrogram_burst/spectrogram/bursts/sub-03/ses-24/ieeg/"
    "sub-03_ses-24_task-pre_bursts.tsv"
)


def _project(root: Path) -> Path:
    flow_dir = root / "code" / "pipelines" / "spectrogram_burst"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").write_text(_SNAKEFILE, encoding="utf-8")
    (flow_dir / "config.yml").write_text(yaml.safe_dump(_CONFIG), encoding="utf-8")
    reg_dir = root / ".projio" / "pipeio"
    reg_dir.mkdir(parents=True)
    reg = {"flows": {"spectrogram_burst": {
        "name": "spectrogram_burst",
        "code_path": "code/pipelines/spectrogram_burst",
        "config_path": "code/pipelines/spectrogram_burst/config.yml",
    }}}
    (reg_dir / "registry.yml").write_text(yaml.safe_dump(reg), encoding="utf-8")
    return flow_dir


def test_pipeline_context_puts_root_before_subject(tmp_path):
    """Regression (issue 20261109-004321): root came after sub-/ses-."""
    from pipeio.resolver import PipelineContext

    _project(tmp_path)
    ctx = PipelineContext.from_registry("spectrogram_burst", root=tmp_path)
    p = ctx.path("bursts", "bursts", subject="03", session="24", task="pre")
    assert p == tmp_path / BURSTS
    # Short names and a v0_0_0 custom entity (acquisition) behave like BidsPaths.
    p = ctx.path("npx", "events", sub="03", ses="24", task="pre", acq="lshank")
    assert p.name == "sub-03_ses-24_task-pre_desc-raw_acquisition-lshank_ev.tsv"
    assert ctx.pattern("bursts", "bursts") == (
        "derivatives/spectrogram_burst/spectrogram/bursts/sub-{subject}/"
        "ses-{session}/ieeg/sub-{subject}_ses-{session}_task-{task}_bursts.tsv"
    )


def test_expand_globs_under_root(tmp_path):
    from pipeio.resolver import PipelineContext

    _project(tmp_path)
    target = tmp_path / BURSTS
    target.parent.mkdir(parents=True)
    target.touch()
    ctx = PipelineContext.from_registry("spectrogram_burst", root=tmp_path)
    assert ctx.expand("bursts", "bursts", subject="03") == [target]
    assert ctx.expand("bursts", "bursts", subject="04") == []


def test_target_paths_resolve_from_config(tmp_path):
    from pipeio.mcp import mcp_target_paths

    _project(tmp_path)
    r = mcp_target_paths(
        tmp_path, "spectrogram_burst", "bursts", "bursts",
        {"subject": "03", "session": "24", "task": "pre"},
    )
    assert r["source"] == "config.yml"
    assert r["path"] == str(tmp_path / BURSTS)
    assert "missing_entities" not in r

    r = mcp_target_paths(tmp_path, "spectrogram_burst", "bursts", "bursts",
                         {"subject": "03"})
    assert r["missing_entities"] == ["session", "task"]


def _fake_python(tmp_path: Path, result: dict, *, fail: bool = False) -> list[str]:
    """A 'project python' that prints a canned Snakefile dump and counts calls."""
    script = tmp_path / "fake_python.py"
    calls = tmp_path / "calls.txt"
    payload = json.dumps(result)
    script.write_text(
        "import sys\n"
        f"open({str(calls)!r}, 'a').write('x')\n"
        + ("sys.stderr.write('boom'); sys.exit(1)\n" if fail else "")
        + f"print('noise from the Snakefile')\nprint('<<<PIPEIO-JSON>>>' + {payload!r})\n",
        encoding="utf-8",
    )
    return [sys.executable, str(script)]


def _dump(root: Path) -> dict:
    reg = dict(_CONFIG["registry"])
    reg["bursts_spindle"] = {
        "base_input": "ieeg",
        "bids": {"root": "spectrogram/bursts_spindle", "datatype": "ieeg"},
        "members": {"bursts": {"suffix": "bursts_spindle", "extension": ".tsv"}},
    }
    return {"spec": "v0_0_0", "paths": {"out_paths": {
        "root_dir": str(root / "derivatives" / "spectrogram_burst"),
        "registry": reg,
        "base_wildcards": {"ieeg": {"subject": "{subject}", "session": "{session}",
                                    "task": "{task}"}},
        "wildcard_sets": None,
    }}}


def test_target_paths_runtime_family_from_snakefile(tmp_path):
    """Families added in the Snakefile resolve via its BidsPaths registry, cached."""
    from pipeio.mcp import mcp_target_paths

    _project(tmp_path)
    py = _fake_python(tmp_path, _dump(tmp_path))
    ent = {"subject": "03", "session": "24", "task": "pre"}
    for _ in range(2):
        r = mcp_target_paths(tmp_path, "spectrogram_burst", "bursts_spindle",
                             "bursts", ent, python_bin=" ".join(py))
        assert r["source"] == "Snakefile", r
        assert r["path"] == str(
            tmp_path / "derivatives/spectrogram_burst/spectrogram/bursts_spindle/"
            "sub-03/ses-24/ieeg/sub-03_ses-24_task-pre_bursts_spindle.tsv"
        )
    assert (tmp_path / "calls.txt").read_text() == "x"  # second call hit the cache

    # Touching the Snakefile invalidates the cache.
    sf = tmp_path / "code/pipelines/spectrogram_burst/Snakefile"
    sf.write_text(sf.read_text() + "\n", encoding="utf-8")
    mcp_target_paths(tmp_path, "spectrogram_burst", "bursts_spindle", "bursts",
                     ent, python_bin=" ".join(py))
    assert (tmp_path / "calls.txt").read_text() == "xx"

    # List mode with snakefile=True includes the runtime family.
    r = mcp_target_paths(tmp_path, "spectrogram_burst", snakefile=True,
                         python_bin=" ".join(py))
    assert "bursts_spindle" in r["groups"]


def test_target_paths_snakefile_failure_is_reported(tmp_path):
    from pipeio.mcp import mcp_target_paths

    _project(tmp_path)
    py = _fake_python(tmp_path, {}, fail=True)
    r = mcp_target_paths(tmp_path, "spectrogram_burst", "bursts_spindle", "bursts",
                         {"subject": "03"}, python_bin=" ".join(py))
    assert "Unknown group 'bursts_spindle'" in r["error"]
    assert "boom" in r["snakefile_error"]
    assert "python_bin" in r["hint"]
