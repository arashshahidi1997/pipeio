"""Tests for pipeio.matlab module."""

from __future__ import annotations

import os

import pytest

from pipeio.matlab import matlab2shell


class TestMatlab2Shell:
    """Tests for matlab2shell()."""

    def test_default_binary(self):
        cmd = matlab2shell("disp('hi')", startup_script=None, reset_path=False)
        assert cmd == 'matlab -batch "disp(\'hi\')"'

    def test_custom_binary(self):
        cmd = matlab2shell(
            "disp('hi')",
            matlab_bin="/opt/MATLAB/R2023a/bin/matlab",
            startup_script=None,
            reset_path=False,
        )
        assert cmd.startswith("/opt/MATLAB/R2023a/bin/matlab -batch")

    def test_env_variable_fallback(self, monkeypatch):
        monkeypatch.setenv("MATLAB_BIN", "/env/matlab")
        cmd = matlab2shell("disp('hi')", startup_script=None, reset_path=False)
        assert cmd.startswith("/env/matlab -batch")

    def test_explicit_overrides_env(self, monkeypatch):
        monkeypatch.setenv("MATLAB_BIN", "/env/matlab")
        cmd = matlab2shell(
            "disp('hi')",
            matlab_bin="/explicit/matlab",
            startup_script=None,
            reset_path=False,
        )
        assert cmd.startswith("/explicit/matlab -batch")

    def test_reset_path(self):
        cmd = matlab2shell("disp('hi')", startup_script=None, reset_path=True)
        assert "restoredefaultpath" in cmd
        assert "rehash toolboxcache" in cmd

    def test_startup_script(self):
        cmd = matlab2shell("disp('hi')", startup_script="code/startup.m", reset_path=False)
        assert "run('code/startup.m')" in cmd

    def test_default_prelude(self):
        """Default: reset_path=True, startup_script='code/startup.m'."""
        cmd = matlab2shell("myFunc(1,2)")
        assert "restoredefaultpath" in cmd
        assert "run('code/startup.m')" in cmd
        assert "myFunc(1,2)" in cmd

    def test_no_prelude(self):
        cmd = matlab2shell("myFunc(1,2)", startup_script=None, reset_path=False)
        assert cmd == 'matlab -batch "myFunc(1,2)"'

    def test_snakemake_pattern(self):
        """Verify the command works with typical Snakemake wildcard syntax."""
        cmd = matlab2shell(
            "functionBrainStateClass_pipeline(pwd,'{wildcards.rec}',212,2500,'{output.mat}','{output.png}')",
            matlab_bin="/usr/local/bin/matlab",
        )
        assert "{wildcards.rec}" in cmd
        assert "{output.mat}" in cmd
        assert cmd.endswith('"')

    def test_import_from_package(self):
        """matlab2shell is importable from the top-level pipeio package."""
        from pipeio import matlab2shell as m2s
        assert callable(m2s)
