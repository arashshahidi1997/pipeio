"""Tests for pipeio.resolver — SimpleResolver, PipelineContext, Session, Stage, InputStage."""

from pathlib import Path

import pytest
import yaml

from pipeio.config import FlowConfig, RegistryGroup, RegistryMember
from pipeio.resolver import InputStage, PipelineContext, Session, SimpleResolver, Stage


def _sample_flow_config() -> FlowConfig:
    return FlowConfig(
        input_dir="raw",
        output_dir="derivatives/preprocess",
        registry={"raw_zarr": RegistryGroup(
                base_input="ieeg",
                bids={"root": "raw_zarr", "datatype": "ieeg"},
                members={"zarr": RegistryMember(suffix="ieeg", extension=".zarr"),
                }),
            "badlabel": RegistryGroup(
                base_input="ieeg",
                bids={"root": "badlabel", "datatype": "ieeg"},
                members={"npy": RegistryMember(suffix="ieeg", extension=".npy"),
                    "featuremap": RegistryMember(suffix="ieeg", extension=".featuremap.png"),
                }),
        })


# ---- SimpleResolver ----


class TestSimpleResolver:
    def test_resolve_basic(self, tmp_path):
        cfg = _sample_flow_config()
        resolver = SimpleResolver(cfg, tmp_path)
        p = resolver.resolve("badlabel", "npy", subject="01", session="pre")
        assert "badlabel" in str(p)
        assert "ieeg.npy" in str(p)
        assert "sub-01" in str(p)
        assert "ses-pre" in str(p)

    def test_resolve_unknown_group(self, tmp_path):
        cfg = _sample_flow_config()
        resolver = SimpleResolver(cfg, tmp_path)
        with pytest.raises(KeyError, match="Unknown group"):
            resolver.resolve("nonexistent", "npy")

    def test_resolve_unknown_member(self, tmp_path):
        cfg = _sample_flow_config()
        resolver = SimpleResolver(cfg, tmp_path)
        with pytest.raises(KeyError, match="Unknown member"):
            resolver.resolve("badlabel", "nonexistent")

    def test_expand_empty(self, tmp_path):
        cfg = _sample_flow_config()
        resolver = SimpleResolver(cfg, tmp_path)
        assert resolver.expand("badlabel", "npy") == []

    def test_expand_finds_files(self, tmp_path):
        cfg = _sample_flow_config()
        resolver = SimpleResolver(cfg, tmp_path)

        # Create matching files
        out = tmp_path / "derivatives/preprocess/badlabel/sub-01"
        out.mkdir(parents=True)
        (out / "sub-01_ieeg.npy").touch()
        (out / "sub-01_ieeg.featuremap.png").touch()

        matches = resolver.expand("badlabel", "npy")
        assert len(matches) == 1
        assert matches[0].suffix == ".npy"

    def test_expand_with_filter(self, tmp_path):
        cfg = _sample_flow_config()
        resolver = SimpleResolver(cfg, tmp_path)

        base = tmp_path / "derivatives/preprocess/badlabel"
        for subj in ["sub-01", "sub-02"]:
            d = base / subj
            d.mkdir(parents=True)
            (d / f"{subj}_ieeg.npy").touch()

        all_matches = resolver.expand("badlabel", "npy")
        assert len(all_matches) == 2

        filtered = resolver.expand("badlabel", "npy", subject="01")
        assert len(filtered) == 1


# ---- PipelineContext ----


class TestPipelineContext:
    def test_from_config(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        assert ctx.root == tmp_path
        assert ctx.groups() == ["badlabel", "raw_zarr"]

    def test_products(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        assert "npy" in ctx.products("badlabel")
        assert "featuremap" in ctx.products("badlabel")

    def test_path(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        p = ctx.path("badlabel", "npy", subject="01")
        assert isinstance(p, Path)
        assert "badlabel" in str(p)

    def test_have_false(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        assert not ctx.have("badlabel", "npy", subject="01")

    def test_have_true(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        p = ctx.path("badlabel", "npy", subject="01")
        p.parent.mkdir(parents=True)
        p.touch()
        assert ctx.have("badlabel", "npy", subject="01")

    def test_pattern(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        patt = ctx.pattern("badlabel", "npy")
        assert "badlabel" in patt
        assert ".npy" in patt

    def test_stage(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        s = ctx.stage("badlabel")
        assert isinstance(s, Stage)
        assert s.name == "badlabel"

    def test_stage_unknown(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        with pytest.raises(KeyError, match="Unknown stage"):
            ctx.stage("nonexistent")

    def test_stage_alias(self, tmp_path):
        cfg = _sample_flow_config()
        cfg.extra["stage_aliases"] = {"labels": "badlabel"}
        ctx = PipelineContext.from_config(cfg, tmp_path)
        s = ctx.stage("labels")
        assert s.name == "badlabel"

    def test_from_registry(self, tmp_path):
        """from_registry should load config via the pipeline registry."""
        cfg = _sample_flow_config()

        # Write a config.yml
        cfg_path = tmp_path / "code" / "pipelines" / "preprocess" / "config.yml"
        cfg_path.parent.mkdir(parents=True)
        cfg_path.write_text(yaml.safe_dump({"input_dir": "raw",
            "output_dir": "derivatives/preprocess",
            "registry": {"badlabel": {"bids": {"root": "badlabel"},
                    "members": {"npy": {"suffix": "ieeg", "extension": ".npy"}},
                },
            },
        }))

        # Write a registry
        reg_dir = tmp_path / ".pipeio"
        reg_dir.mkdir()
        reg_path = reg_dir / "registry.yml"
        reg_path.write_text(yaml.safe_dump({"flows": {"preprocess": {"name": "preprocess",
                    "code_path": "code/pipelines/preprocess",
                    "config_path": "code/pipelines/preprocess/config.yml",
                },
            },
        }))

        ctx = PipelineContext.from_registry("preprocess", root=tmp_path)
        assert "badlabel" in ctx.groups()


# ---- Session ----


class TestSession:
    def test_get(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        sess = ctx.session(subject="01", session="pre")
        p = sess.get("badlabel", "npy")
        assert "sub-01" in str(p)
        assert "ses-pre" in str(p)

    def test_get_with_override(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        sess = ctx.session(subject="01", session="pre")
        p = sess.get("badlabel", "npy", session="post")
        assert "ses-post" in str(p)

    def test_have(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        sess = ctx.session(subject="01")
        assert not sess.have("badlabel", "npy")

    def test_bundle(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        sess = ctx.session(subject="01")
        b = sess.bundle("badlabel")
        assert "npy" in b
        assert "featuremap" in b
        assert all(isinstance(v, Path) for v in b.values())


# ---- Stage ----


class TestStage:
    def test_paths(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stage = ctx.stage("badlabel")
        sess = ctx.session(subject="01")
        paths = stage.paths(sess)
        assert "npy" in paths
        assert "featuremap" in paths

    def test_paths_subset(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stage = ctx.stage("badlabel")
        sess = ctx.session(subject="01")
        paths = stage.paths(sess, members=["npy"])
        assert list(paths.keys()) == ["npy"]

    def test_paths_unknown_member(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stage = ctx.stage("badlabel")
        sess = ctx.session(subject="01")
        with pytest.raises(KeyError, match="has no member"):
            stage.paths(sess, members=["nonexistent"])

    def test_have_false(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stage = ctx.stage("badlabel")
        sess = ctx.session(subject="01")
        assert not stage.have(sess)

    def test_have_true(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stage = ctx.stage("badlabel")
        sess = ctx.session(subject="01")
        # Create all member files
        for member in ["npy", "featuremap"]:
            p = sess.get("badlabel", member)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()
        assert stage.have(sess)

    def test_resolve(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        sess = ctx.session(subject="01")

        # Create raw_zarr files so it exists
        p = sess.get("raw_zarr", "zarr")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()

        stage = ctx.stage("raw_zarr")
        result = stage.resolve(sess, prefer=["badlabel", "raw_zarr"])
        assert result == "raw_zarr"

    def test_resolve_not_found(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stage = ctx.stage("badlabel")
        sess = ctx.session(subject="01")
        with pytest.raises(FileNotFoundError):
            stage.resolve(sess, prefer=["badlabel", "raw_zarr"])


# ---- InputStage ----


def _sample_flow_config_with_inputs() -> FlowConfig:
    """FlowConfig with pybids_inputs for input stage testing."""
    return FlowConfig(
        input_dir="raw",
        output_dir="derivatives/preprocess",
        registry={
            "interpolate": RegistryGroup(
                base_input="ieeg",
                bids={"root": "interpolate", "datatype": "ieeg"},
                members={"lfp": RegistryMember(suffix="ieeg", extension=".lfp")},
            ),
        },
        extra={
            "pybids_inputs": {
                "ieeg": {
                    "filters": {
                        "suffix": "ieeg",
                        "extension": ".lfp",
                        "datatype": "ieeg",
                    },
                    "wildcards": ["subject", "session", "task"],
                },
                "ecephys": {
                    "filters": {
                        "suffix": "ecephys",
                        "extension": ".lfp",
                        "recording": "lf",
                    },
                    "wildcards": ["subject", "session", "task", "acquisition", "recording"],
                },
            },
        },
    )


class TestInputStage:
    def test_stage_returns_input_stage(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        assert isinstance(stg, InputStage)
        assert stg.name == "raw"

    def test_input_stages_list(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        assert ctx.input_stages() == ["raw"]

    def test_input_stages_empty_without_pybids(self, tmp_path):
        cfg = _sample_flow_config()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        assert ctx.input_stages() == []

    def test_members(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        assert stg.members() == ["ecephys", "ieeg"]

    def test_paths(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        sess = ctx.session(subject="01", session="04", task="free")
        paths = stg.paths(sess)
        assert "ieeg" in paths
        assert "ecephys" in paths
        # Verify BIDS path structure for ieeg
        p = paths["ieeg"]
        assert "raw" in str(p)
        assert "sub-01" in str(p)
        assert "ses-04" in str(p)
        assert "ieeg" in str(p)  # datatype dir
        assert p.name == "sub-01_ses-04_task-free_ieeg.lfp"

    def test_paths_subset(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        sess = ctx.session(subject="01", session="04", task="free")
        paths = stg.paths(sess, members=["ieeg"])
        assert list(paths.keys()) == ["ieeg"]

    def test_paths_unknown_member(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        sess = ctx.session(subject="01", session="04", task="free")
        with pytest.raises(KeyError, match="has no member"):
            stg.paths(sess, members=["nonexistent"])

    def test_have_false(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        sess = ctx.session(subject="01", session="04", task="free")
        assert not stg.have(sess)

    def test_have_true(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        sess = ctx.session(subject="01", session="04", task="free")
        # Create all member files
        for member in ["ieeg", "ecephys"]:
            p = stg.paths(sess, members=[member])[member]
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()
        assert stg.have(sess)

    def test_have_subset(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        sess = ctx.session(subject="01", session="04", task="free")
        # Create only ieeg file
        p = stg.paths(sess, members=["ieeg"])["ieeg"]
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
        assert stg.have(sess, members=["ieeg"])
        assert not stg.have(sess)  # ecephys missing

    def test_resolve_prefers_existing(self, tmp_path):
        """resolve() should return the first stage in prefer order that exists."""
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        sess = ctx.session(subject="01", session="04", task="free")

        # Create raw input files (both members)
        stg = ctx.stage("raw")
        for member in stg.members():
            p = stg.paths(sess, members=[member])[member]
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()

        # raw should be resolved since interpolate doesn't exist
        result = stg.resolve(sess, prefer=["interpolate", "raw"])
        assert result == "raw"

    def test_resolve_across_input_and_output(self, tmp_path):
        """resolve() should work across input and output stages."""
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        sess = ctx.session(subject="01", session="04", task="free")

        # Create interpolate output files
        interp_stage = ctx.stage("interpolate")
        for member in ctx.products("interpolate"):
            p = sess.get("interpolate", member)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()

        # interpolate exists, so should be picked first
        stg = ctx.stage("raw")
        result = stg.resolve(sess, prefer=["interpolate", "raw"])
        assert result == "interpolate"

    def test_resolve_not_found(self, tmp_path):
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("raw")
        sess = ctx.session(subject="01", session="04", task="free")
        with pytest.raises(FileNotFoundError):
            stg.resolve(sess, prefer=["interpolate", "raw"])

    def test_stage_alias_to_input(self, tmp_path):
        """stage_aliases should work for input stages too."""
        cfg = _sample_flow_config_with_inputs()
        cfg.extra["stage_aliases"] = {"source": "raw"}
        ctx = PipelineContext.from_config(cfg, tmp_path)
        stg = ctx.stage("source")
        assert isinstance(stg, InputStage)
        assert stg.name == "raw"

    def test_unknown_stage_error_includes_input_stages(self, tmp_path):
        """KeyError for unknown stage should mention input stages."""
        cfg = _sample_flow_config_with_inputs()
        ctx = PipelineContext.from_config(cfg, tmp_path)
        with pytest.raises(KeyError, match="raw"):
            ctx.stage("nonexistent")
