"""Tests for pipeio.config."""

from pathlib import Path

import pytest
import yaml

from pipeio.config import FlowConfig, RegistryGroup, RegistryMember


def _sample_config_yaml() -> str:
    return """\
input_dir: "raw"
input_registry: "raw/registry.yml"
input_dir_brainstate: "derivatives"
input_registry_brainstate: "derivatives/brainstate/registry.yml"
output_dir: "derivatives/preprocess"
output_registry: "derivatives/preprocess/registry.yml"

pybids_inputs:
  ieeg:
    filters:
      suffix: 'ieeg'
    wildcards:
      - subject

registry:
  raw_zarr:
    base_input: "ieeg"
    bids:
      root: "raw_zarr"
      datatype: "ieeg"
    members:
      zarr: { suffix: "ieeg", extension: ".zarr" }
  badlabel:
    base_input: "ieeg"
    bids:
      root: "badlabel"
      datatype: "ieeg"
    members:
      npy: { suffix: "ieeg", extension: ".npy" }
      featuremap: { suffix: "ieeg", extension: ".featuremap.png" }
"""


def test_from_yaml(tmp_path):
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(_sample_config_yaml())
    cfg = FlowConfig.from_yaml(cfg_path)

    assert cfg.input_dir == "raw"
    assert cfg.output_dir == "derivatives/preprocess"
    assert "raw_zarr" in cfg.registry
    assert "badlabel" in cfg.registry


def test_extra_fields(tmp_path):
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(_sample_config_yaml())
    cfg = FlowConfig.from_yaml(cfg_path)

    assert "pybids_inputs" in cfg.extra
    assert "input_dir_brainstate" in cfg.extra


def test_extra_inputs(tmp_path):
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(_sample_config_yaml())
    cfg = FlowConfig.from_yaml(cfg_path)

    extras = cfg.extra_inputs()
    assert "brainstate" in extras
    dir_path, reg_path = extras["brainstate"]
    assert dir_path == "derivatives"
    assert "brainstate" in reg_path


def test_groups(tmp_path):
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(_sample_config_yaml())
    cfg = FlowConfig.from_yaml(cfg_path)

    groups = cfg.groups()
    assert "badlabel" in groups
    assert "raw_zarr" in groups


def test_products(tmp_path):
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(_sample_config_yaml())
    cfg = FlowConfig.from_yaml(cfg_path)

    prods = cfg.products("badlabel")
    assert "npy" in prods
    assert "featuremap" in prods


def test_products_unknown_group(tmp_path):
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(_sample_config_yaml())
    cfg = FlowConfig.from_yaml(cfg_path)

    with pytest.raises(KeyError, match="Unknown group"):
        cfg.products("nonexistent")


def test_validate_config_valid(tmp_path):
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(_sample_config_yaml())
    cfg = FlowConfig.from_yaml(cfg_path)

    issues = cfg.validate_config()
    assert issues == []


def test_validate_config_empty_dirs():
    cfg = FlowConfig()
    issues = cfg.validate_config()
    assert any("input_dir" in i for i in issues)
    assert any("output_dir" in i for i in issues)


def test_validate_config_empty_group():
    cfg = FlowConfig(
        input_dir="raw",
        output_dir="out",
        registry={"empty": RegistryGroup()},
    )
    issues = cfg.validate_config()
    assert any("no members" in i for i in issues)
