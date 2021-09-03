"""
Copyright (c) 2015 Red Hat, Inc
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
"""
import os
import subprocess
import tempfile
import tarfile

from atomic_reactor.plugin import PostBuildPlugin
from atomic_reactor.utils.rpm import rpm_qf_args, parse_rpm_output
from docker.errors import APIError

RPMDB_PATH = '/var/lib/rpm'
RPMDB_DIR_NAME = 'rpm'

__all__ = ('PostBuildRPMqaPlugin', )


class PostBuildRPMqaPlugin(PostBuildPlugin):
    key = "all_rpm_packages"
    is_allowed_to_fail = False
    sep = ';'

    def __init__(self, tasker, workflow, image_id, ignore_autogenerated_gpg_keys=True):
        """
        constructor

        :param tasker: ContainerTasker instance
        :param workflow: DockerBuildWorkflow instance
        """
        # call parent constructor
        super(PostBuildRPMqaPlugin, self).__init__(tasker, workflow)
        self.image_id = image_id
        self.ignore_autogenerated_gpg_keys = ignore_autogenerated_gpg_keys

        self._container_ids = []

    def run(self):
        # If another component has already filled in the image component list, skip
        if self.workflow.image_components is not None:
            return None

        plugin_output = self.gather_output()

        if self.workflow.dockerfile_images.base_from_scratch:
            if not plugin_output:
                self.tasker.cleanup_containers(*self._container_ids)
                return None

        # gpg-pubkey are autogenerated packages by rpm when you import a gpg key
        # these are of course not signed, let's ignore those by default
        if self.ignore_autogenerated_gpg_keys:
            self.log.debug("ignore rpms 'gpg-pubkey'")
            plugin_output = [x for x in plugin_output if not x.startswith("gpg-pubkey" + self.sep)]

        self.tasker.cleanup_containers(*self._container_ids)

        self.workflow.image_components = parse_rpm_output(plugin_output)

        return plugin_output

    def gather_output(self):
        container_dict = self.tasker.create_container(self.image_id, command=['/bin/bash'])
        container_id = container_dict['Id']
        self._container_ids.append(container_id)

        try:
            bits, _ = self.tasker.get_archive(container_id, RPMDB_PATH)
        except APIError as ex:
            self.log.info('Could not extract rpmdb in %s : %s', RPMDB_PATH, ex)
            if self.workflow.dockerfile_images.base_from_scratch:
                return None
            raise RuntimeError(ex) from ex

        except Exception as ex:
            self.log.info('Get archive failed while extracting rpmdb in %s : %s', RPMDB_PATH, ex)
            raise RuntimeError(ex) from ex

        with tempfile.NamedTemporaryFile() as rpmdb_archive:
            for chunk in bits:
                rpmdb_archive.write(chunk)
            rpmdb_archive.flush()
            tar_archive = tarfile.TarFile(rpmdb_archive.name)

        with tempfile.TemporaryDirectory() as rpmdb_dir:
            tar_archive.extractall(rpmdb_dir)

            rpmdb_path = os.path.join(rpmdb_dir, RPMDB_DIR_NAME)

            if not os.listdir(rpmdb_path):
                self.log.info('rpmdb directory %s is empty', RPMDB_PATH)
                if self.workflow.dockerfile_images.base_from_scratch:
                    return None
                raise RuntimeError(f'rpmdb directory {RPMDB_PATH} is empty')

            rpm_cmd = 'rpm --dbpath {} {}'.format(rpmdb_path, rpm_qf_args())
            try:
                self.log.info('getting rpms from rpmdb: %s', rpm_cmd)
                rpm_output = subprocess.check_output(rpm_cmd,
                                                     shell=True, universal_newlines=True)  # nosec
            except Exception as e:
                self.log.error("Failed to get rpms from rpmdb: %s", e)
                raise e

        rpm_output = [line for line in rpm_output.splitlines() if line]
        return rpm_output
