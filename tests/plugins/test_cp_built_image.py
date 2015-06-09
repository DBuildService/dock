"""
Copyright (c) 2015 Red Hat, Inc
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
"""

from __future__ import unicode_literals

import os
import subprocess

import pytest
from flexmock import flexmock

from dock.util import ImageName
from dock.core import DockerTasker
from dock.inner import DockerBuildWorkflow
from dock.plugin import PostBuildPluginsRunner
from dock.plugins import post_cp_built_image
from dock.plugins.post_cp_built_image import CopyBuiltImageToNFSPlugin
from tests.constants import INPUT_IMAGE


class Y(object):
    pass


class X(object):
    image_id = INPUT_IMAGE
    source = Y()
    source.dockerfile_path = None
    source.path = None
    base_image = ImageName(repo="qwe", tag="asd")


NFS_SERVER_PATH = "server:path"


@pytest.mark.parametrize('dest_dir', [None, "test_directory"])
def test_cp_built_image(tmpdir, dest_dir):
    mountpoint = tmpdir.join("mountpoint")

    def fake_check_call(cmd):
        assert cmd == [
            "mount",
            "-t", "nfs",
            "-o", "nolock",
            NFS_SERVER_PATH,
            mountpoint,
        ]
    flexmock(subprocess, check_call=fake_check_call)
    tasker = DockerTasker()
    workflow = DockerBuildWorkflow({"provider": "git", "uri": "asd"}, "test-image")
    workflow.builder = X()
    workflow.exported_squashed_image = {"path": os.path.join(str(tmpdir), "image.tar")}
    open(workflow.exported_squashed_image.get("path"), 'a').close()

    runner = PostBuildPluginsRunner(
        tasker,
        workflow,
        [{
            'name': CopyBuiltImageToNFSPlugin.key,
            'args': {
                "nfs_server_path": NFS_SERVER_PATH,
                "dest_dir": dest_dir,
                "mountpoint": str(mountpoint),
            }
        }]
    )
    runner.run()
    if dest_dir is None:
        assert os.path.isfile(os.path.join(str(mountpoint), "image.tar"))
    else:
        assert os.path.isfile(os.path.join(str(mountpoint), dest_dir, "image.tar"))
