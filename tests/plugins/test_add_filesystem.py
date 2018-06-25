"""
Copyright (c) 2016 Red Hat, Inc
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
"""

from __future__ import print_function, unicode_literals
from textwrap import dedent
from flexmock import flexmock

import pytest
import os.path
import responses
import logging

try:
    import koji
except ImportError:
    import inspect
    import sys

    # Find our mocked koji module
    import tests.koji as koji
    mock_koji_path = os.path.dirname(inspect.getfile(koji.ClientSession))
    if mock_koji_path not in sys.path:
        sys.path.append(os.path.dirname(mock_koji_path))

    # Now load it properly, the same way the plugin will
    del koji
    import koji

from atomic_reactor.inner import DockerBuildWorkflow
from atomic_reactor.plugin import (
    PreBuildPluginsRunner, PluginFailedException, BuildCanceledException)
from atomic_reactor.plugins.pre_add_filesystem import AddFilesystemPlugin
from atomic_reactor.plugins.pre_reactor_config import (ReactorConfigPlugin,
                                                       WORKSPACE_CONF_KEY,
                                                       ReactorConfig)
from atomic_reactor.util import ImageName, df_parser
from atomic_reactor.source import VcsInfo
from atomic_reactor.constants import PLUGIN_ADD_FILESYSTEM_KEY, PLUGIN_CHECK_AND_SET_PLATFORMS_KEY
from atomic_reactor import koji_util, util
from tests.constants import (MOCK_SOURCE, DOCKERFILE_GIT, DOCKERFILE_SHA1,
                             MOCK, IMPORTED_IMAGE_ID)
from tests.fixtures import docker_tasker, reactor_config_map  # noqa
if MOCK:
    from tests.docker_mock import mock_docker
    from tests.retry_mock import mock_get_retry_session

KOJI_HUB = 'https://koji-hub.com'
FILESYSTEM_TASK_ID = 1234567

DEFAULT_DOCKERFILE = dedent("""\
    FROM koji/image-build
    RUN dnf install -y python-django
    """)


class MockSource(object):
    def __init__(self, tmpdir):
        tmpdir = str(tmpdir)
        self.dockerfile_path = os.path.join(tmpdir, 'Dockerfile')
        self.path = tmpdir

    def get_build_file_path(self):
        return self.dockerfile_path, self.path

    def get_vcs_info(self):
        return VcsInfo('git', DOCKERFILE_GIT, DOCKERFILE_SHA1)


class X(object):
    image_id = "xxx"
    base_image = ImageName.parse("koji/image-build")
    set_base_image = flexmock()


def mock_koji_session(koji_proxyuser=None, koji_ssl_certs_dir=None,
                      koji_krb_principal=None, koji_krb_keytab=None,
                      scratch=False, image_task_fail=False,
                      throws_build_cancelled=False,
                      error_on_build_cancelled=False,
                      download_filesystem=True,
                      get_task_result_mock=None,
                      arches=None):

    session = flexmock()

    def _mockBuildImageOz(*args, **kwargs):
        if scratch:
            assert kwargs['opts']['scratch'] is True
        else:
            assert 'scratch' not in kwargs['opts']

        if arches:
            assert set(args[2]) == set(arches)

        if not download_filesystem:
            return None

        return FILESYSTEM_TASK_ID

    flexmock(util).should_receive('is_scratch_build').and_return(scratch)
    session.should_receive('buildImageOz').replace_with(_mockBuildImageOz)

    session.should_receive('taskFinished').and_return(True)
    if image_task_fail:
        session.should_receive('getTaskInfo').and_return({
            'state': koji_util.koji.TASK_STATES['FAILED']
        })
    else:
        session.should_receive('getTaskInfo').and_return({
            'state': koji_util.koji.TASK_STATES['CLOSED']
        })

    if get_task_result_mock:
        (session.should_receive('getTaskResult')
            .replace_with(get_task_result_mock).once())

    session.should_receive('listTaskOutput').and_return([
        'fedora-23-1.0.x86_64.tar.gz',
    ])
    session.should_receive('getTaskChildren').and_return([
        {'id': 1234568},
    ])
    if download_filesystem:
        session.should_receive('downloadTaskOutput').and_return('tarball-contents')
    else:
        session.should_receive('downloadTaskOutput').never()
    session.should_receive('krb_login').and_return(True)

    if throws_build_cancelled:
        task_watcher = flexmock(koji_util.TaskWatcher)

        task_watcher.should_receive('wait').and_raise(BuildCanceledException)
        task_watcher.should_receive('failed').and_return(True)

        cancel_mock_chain = session.should_receive('cancelTask').\
            with_args(FILESYSTEM_TASK_ID).once()

        if error_on_build_cancelled:
            cancel_mock_chain.and_raise(Exception("foo"))

    (flexmock(koji)
        .should_receive('ClientSession')
        .once()
        .and_return(session))


def mock_image_build_file(tmpdir, contents=None):
    file_path = os.path.join(tmpdir, 'image-build.conf')

    if contents is None:
        contents = dedent("""\
            [image-build]
            name = fedora-23
            version = 1.0
            target = guest-fedora-23-docker
            install_tree = http://install-tree.com/$arch/fedora23/

            format = docker
            distro = Fedora-23
            repo = http://repo.com/fedora/$arch/os/

            ksurl = git+http://ksrul.com/git/spin-kickstarts.git?fedora23#b232f73e
            ksversion = FEDORA23
            kickstart = fedora-23.ks

            [factory-parameters]
            create_docker_metadata = False

            [ova-options]
            ova_option_1 = ova_option_1_value
            """)

    with open(file_path, 'w') as f:
        f.write(dedent(contents))

    return file_path


def mock_workflow(tmpdir, dockerfile=DEFAULT_DOCKERFILE):
    workflow = DockerBuildWorkflow(MOCK_SOURCE, 'test-image')
    mock_source = MockSource(tmpdir)
    setattr(workflow, 'builder', X)
    workflow.builder.source = mock_source
    flexmock(workflow, source=mock_source)

    df = df_parser(str(tmpdir))
    df.content = dockerfile
    setattr(workflow.builder, 'df_path', df.dockerfile_path)
    mock_get_retry_session()

    return workflow


def create_plugin_instance(tmpdir, kwargs=None, scratch=False, reactor_config_map=False):  # noqa
    flexmock(util).should_receive('is_scratch_build').and_return(scratch)
    tasker = flexmock()
    workflow = mock_workflow(tmpdir)

    if kwargs is None:
        kwargs = {}

    if reactor_config_map:
        make_and_store_reactor_config_map(workflow, {'root_url': kwargs.get('url', '')})

    return AddFilesystemPlugin(tasker, workflow, KOJI_HUB, **kwargs)


def make_and_store_reactor_config_map(workflow, additional_koji=None):
    workflow.plugin_workspace[ReactorConfigPlugin.key] = {}

    reactor_map = {
        'version': 1,
        'koji': {'hub_url': KOJI_HUB}
    }
    if additional_koji:
        reactor_map['koji'].update(additional_koji)

    workflow.plugin_workspace[ReactorConfigPlugin.key] = {
        WORKSPACE_CONF_KEY: ReactorConfig(reactor_map)
    }


@pytest.mark.parametrize('scratch', [True, False])
def test_add_filesystem_plugin_generated(tmpdir, docker_tasker, scratch, reactor_config_map):
    if MOCK:
        mock_docker()

    workflow = mock_workflow(tmpdir)
    task_id = FILESYSTEM_TASK_ID
    mock_koji_session(scratch=scratch)
    mock_image_build_file(str(tmpdir))

    if reactor_config_map:
        make_and_store_reactor_config_map(workflow, {'root_url': '', 'auth': {}})

    runner = PreBuildPluginsRunner(
        docker_tasker,
        workflow,
        [{
            'name': PLUGIN_ADD_FILESYSTEM_KEY,
            'args': {
                'koji_hub': KOJI_HUB,
                'from_task_id': task_id,
                'architecture': 'x86_64'
            }
        }]
    )

    expected_results = {
        'base-image-id': IMPORTED_IMAGE_ID,
        'filesystem-koji-task-id': FILESYSTEM_TASK_ID,
    }
    results = runner.run()
    plugin_result = results[PLUGIN_ADD_FILESYSTEM_KEY]
    assert 'base-image-id' in plugin_result
    assert 'filesystem-koji-task-id' in plugin_result
    assert plugin_result == expected_results


@pytest.mark.parametrize('scratch', [True, False])
def test_add_filesystem_plugin_legacy(tmpdir, docker_tasker, scratch, reactor_config_map):
    if MOCK:
        mock_docker()

    workflow = mock_workflow(tmpdir)
    mock_koji_session(scratch=scratch)
    mock_image_build_file(str(tmpdir))

    if reactor_config_map:
        make_and_store_reactor_config_map(workflow, {'root_url': '', 'auth': {}})

    runner = PreBuildPluginsRunner(
        docker_tasker,
        workflow,
        [{
            'name': PLUGIN_ADD_FILESYSTEM_KEY,
            'args': {
                'koji_hub': KOJI_HUB,
            }
        }]
    )

    results = runner.run()
    plugin_result = results[PLUGIN_ADD_FILESYSTEM_KEY]
    assert 'base-image-id' in plugin_result
    assert plugin_result['base-image-id'] == IMPORTED_IMAGE_ID
    assert 'filesystem-koji-task-id' in plugin_result


@pytest.mark.parametrize(('global_arches', 'param_arches', 'expected_arches'), (
    (['x86_64'], None, ['x86_64']),
    (None, ['x86_64'], ['x86_64']),
    (['x86_64', 'ppc64le'], None, ['x86_64', 'ppc64le']),
    (None, ['x86_64', 'ppc64le'], ['x86_64', 'ppc64le']),
    (['x86_64'], ['spam'], ['x86_64']),
    (['x86_64', 'ppc64le'], ['spam', 'bacon'], ['x86_64', 'ppc64le']),
))
def test_use_check_and_set_platforms_result(tmpdir, docker_tasker, global_arches, param_arches,
                                            expected_arches):
    """
    global_arches: list of architectures returned by check_and_set_platforms plugin
    param_arches: list of architectures given to add_filesystem plugin as parameter
    """
    if MOCK:
        mock_docker()

    workflow = mock_workflow(tmpdir)
    mock_koji_session(arches=expected_arches)
    mock_image_build_file(str(tmpdir))

    if global_arches:
        workflow.prebuild_results[PLUGIN_CHECK_AND_SET_PLATFORMS_KEY] = set(global_arches)

    plugin_args = {}
    if param_arches:
        plugin_args['architectures'] = param_arches

    runner = PreBuildPluginsRunner(
        docker_tasker,
        workflow,
        [{
            'name': PLUGIN_ADD_FILESYSTEM_KEY,
            'args': plugin_args
        }]
    )

    results = runner.run()
    plugin_result = results[PLUGIN_ADD_FILESYSTEM_KEY]
    assert 'base-image-id' in plugin_result
    assert plugin_result['base-image-id'] is None
    assert 'filesystem-koji-task-id' in plugin_result


@pytest.mark.parametrize(('base_image', 'type_match'), [
    ('koji/image-build', True),
    ('KoJi/ImAgE-bUiLd  \n', True),
    ('spam/bacon', False),
    ('SpAm/BaCon  \n', False),
])
def test_base_image_type(tmpdir, base_image, type_match, reactor_config_map):
    plugin = create_plugin_instance(tmpdir, reactor_config_map=reactor_config_map)
    assert plugin.is_image_build_type(base_image) == type_match


def test_image_build_file_parse(tmpdir, reactor_config_map):  # noqa
    plugin = create_plugin_instance(tmpdir, reactor_config_map=reactor_config_map)
    file_name = mock_image_build_file(str(tmpdir))
    image_name, config, opts = plugin.parse_image_build_config(file_name)
    assert image_name == 'fedora-23'
    assert config == [
        'fedora-23',
        '1.0',
        ['x86_64'],
        'guest-fedora-23-docker',
        'http://install-tree.com/$arch/fedora23/'
    ]
    assert opts['opts'] == {
        'disk_size': 10,
        'distro': 'Fedora-23',
        'factory_parameter': [('create_docker_metadata', 'False')],
        'ova_option': ['ova_option_1=ova_option_1_value'],
        'format': ['docker'],
        'kickstart': 'fedora-23.ks',
        'ksurl': 'git+http://ksrul.com/git/spin-kickstarts.git?fedora23#b232f73e',
        'ksversion': 'FEDORA23',
        'repo': ['http://repo.com/fedora/$arch/os/'],
    }


def test_missing_yum_repourls(tmpdir, reactor_config_map):  # noqa
    plugin = create_plugin_instance(tmpdir, {'repos': None}, reactor_config_map=reactor_config_map)
    image_build_conf = dedent("""\
        [image-build]
        version = 1.0
        target = guest-fedora-23-docker

        distro = Fedora-23

        ksversion = FEDORA23
        """)

    file_name = mock_image_build_file(str(tmpdir), contents=image_build_conf)
    with pytest.raises(ValueError) as exc:
        plugin.parse_image_build_config(file_name)
    assert 'install_tree cannot be empty' in str(exc)


@pytest.mark.parametrize(('build_cancel', 'error_during_cancel'), [
    (True, False),
    (True, True),
    (False, False),
])
@pytest.mark.parametrize('raise_error', [True, False])
def test_image_task_failure(tmpdir, build_cancel, error_during_cancel, raise_error, caplog,
                            reactor_config_map):
    if MOCK:
        mock_docker()

    task_result = 'task-result'

    def _mockGetTaskResult(task_id):
        if raise_error:
            raise RuntimeError(task_result)
        return task_result
    workflow = mock_workflow(tmpdir)
    mock_koji_session(image_task_fail=True,
                      throws_build_cancelled=build_cancel,
                      error_on_build_cancelled=error_during_cancel,
                      get_task_result_mock=_mockGetTaskResult)
    mock_image_build_file(str(tmpdir))

    if reactor_config_map:
        make_and_store_reactor_config_map(workflow, {'root_url': '', 'auth': {}})

    runner = PreBuildPluginsRunner(
        docker_tasker,
        workflow,
        [{
            'name': PLUGIN_ADD_FILESYSTEM_KEY,
            'args': {'koji_hub': KOJI_HUB, 'architectures': ['x86_64']}
        }]
    )

    with caplog.atLevel(logging.INFO), pytest.raises(PluginFailedException) as exc:
        runner.run()

    assert task_result in str(exc)
    # Also ensure getTaskResult exception message is wrapped properly
    assert 'image task failed' in str(exc)

    if build_cancel:
        msg = "Build was canceled, canceling task %s" % FILESYSTEM_TASK_ID
        assert msg in [x.message for x in caplog.records()]

        if error_during_cancel:
            # We're checking last but one message, as the last one is
            # 'plugin 'add_filesystem' raised an exception'
            assert "Exception while canceling a task (ignored): Exception("\
                in caplog.records()[-2].message
        else:
            msg = "task %s canceled" % FILESYSTEM_TASK_ID
            assert msg in [x.message for x in caplog.records()]


# with a task_id is the new standard, None is legacy-mode support
@pytest.mark.parametrize('task_id', [FILESYSTEM_TASK_ID, None])
@responses.activate
def test_image_build_defaults(tmpdir, task_id, reactor_config_map):
    repos = [
        'http://install-tree.com/fedora23.repo',
        'http://repo.com/fedora/os.repo',
    ]
    responses.add(responses.GET, 'http://install-tree.com/fedora23.repo',
                  body=dedent("""\
                    [fedora-23]
                    baseurl = http://install-tree.com/$basearch/fedora23
                    """))
    responses.add(responses.GET, 'http://repo.com/fedora/os.repo',
                  body=dedent("""\
                    [fedora-os]
                    baseurl = http://repo.com/fedora/$basearch/os

                    [fedora-os2]
                    baseurl = http://repo.com/fedora/$basearch/os2
                    """))
    plugin = create_plugin_instance(tmpdir, {'repos': repos, 'from_task_id': task_id},
                                    reactor_config_map=reactor_config_map)
    image_build_conf = dedent("""\
        [image-build]
        version = 1.0
        target = guest-fedora-23-docker

        distro = Fedora-23

        ksversion = FEDORA23
        """)

    file_name = mock_image_build_file(str(tmpdir), contents=image_build_conf)
    image_name, config, opts = plugin.parse_image_build_config(file_name)
    assert image_name == 'default-name'
    assert config == [
        'default-name',
        '1.0',
        ['x86_64'],
        'guest-fedora-23-docker',
        'http://install-tree.com/$arch/fedora23',
    ]
    assert opts['opts'] == {
        'disk_size': 10,
        'distro': 'Fedora-23',
        'factory_parameter': [('create_docker_metadata', 'False')],
        'format': ['docker'],
        'kickstart': 'kickstart.ks',
        'ksurl': '{}#{}'.format(DOCKERFILE_GIT, DOCKERFILE_SHA1),
        'ksversion': 'FEDORA23',
        'repo': [
            'http://install-tree.com/$arch/fedora23',
            'http://repo.com/fedora/$arch/os',
            'http://repo.com/fedora/$arch/os2',
        ],
    }


@pytest.mark.parametrize(('architectures', 'architecture'), [
    (None, None),
    (['x86_64', 'aarch64', 'ppc64le'], None),
    (None, 'x86_64'),
])
@responses.activate
def test_image_build_overwrites(tmpdir, architectures, architecture, reactor_config_map):
    repos = [
        'http://default-install-tree.com/fedora23.repo',
        'http://default-repo.com/fedora/os.repo',
    ]
    responses.add(responses.GET, 'http://default-install-tree.com/fedora23.repo',
                  body=dedent("""\
                    [fedora-23]
                    baseurl = http://default-install-tree.com/$basearch/fedora23
                    """))
    responses.add(responses.GET, 'http://default-repo.com/fedora/os.repo',
                  body=dedent("""\
                    [fedora-os]
                    baseurl = http://default-repo.com/fedora/$basearch/os.repo
                    """))
    plugin = create_plugin_instance(tmpdir, {
        'repos': repos,
        'architectures': architectures,
        'architecture': architecture
    }, reactor_config_map=reactor_config_map)
    image_build_conf = dedent("""\
        [image-build]
        name = my-name
        version = 1.0
        arches = i386,i486
        target = guest-fedora-23-docker
        install_tree = http://install-tree.com/$arch/fedora23/
        format = locker,mocker
        disk_size = 20

        distro = Fedora-23
        repo = http://install-tree.com/$arch/fedora23/,http://repo.com/fedora/$arch/os/

        ksurl = http://ksurl#123
        kickstart = my-kickstart.ks
        ksversion = FEDORA23

        [factory-parameters]
        create_docker_metadata = Maybe
        """)

    file_name = mock_image_build_file(str(tmpdir), contents=image_build_conf)
    image_name, config, opts = plugin.parse_image_build_config(file_name)
    assert image_name == 'my-name'
    if architectures:
        config_arch = architectures
    elif architecture:
        config_arch = [architecture]
    else:
        config_arch = ['i386', 'i486']
    assert config == [
        'my-name',
        '1.0',
        config_arch,
        'guest-fedora-23-docker',
        'http://install-tree.com/$arch/fedora23/',
    ]
    assert opts['opts'] == {
        'disk_size': 20,
        'distro': 'Fedora-23',
        'factory_parameter': [('create_docker_metadata', 'Maybe')],
        'format': ['locker', 'mocker'],
        'kickstart': 'my-kickstart.ks',
        'ksurl': 'http://ksurl#123',
        'ksversion': 'FEDORA23',
        'repo': [
            'http://install-tree.com/$arch/fedora23/',
            'http://repo.com/fedora/$arch/os/',
        ],
    }


def test_build_filesystem_missing_conf(tmpdir, reactor_config_map):  # noqa
    plugin = create_plugin_instance(tmpdir, reactor_config_map=reactor_config_map)
    with pytest.raises(RuntimeError) as exc:
        plugin.build_filesystem('image-build.conf')
    assert 'Image build configuration file not found' in str(exc)


@pytest.mark.parametrize(('prefix', 'architecture', 'suffix'), [
    ('fedora-23-spam-', None, '.tar'),
    ('fedora-23-spam-', 'x86_64', '.tar.gz'),
    ('fedora-23-spam-', 'aarch64', '.tar.bz2'),
    ('fedora-23-spam-', None, '.tar.xz'),
])
def test_build_filesystem_from_task_id(tmpdir, prefix, architecture, suffix, reactor_config_map):
    task_id = 987654321
    pattern = '{}{}{}'.format(prefix, architecture, suffix)
    plugin = create_plugin_instance(tmpdir, {
        'from_task_id': task_id,
        'architecture': architecture,
    }, reactor_config_map=reactor_config_map)
    plugin.session = flexmock()
    mock_image_build_file(str(tmpdir))
    task_id, filesystem_regex = plugin.build_filesystem('image-build.conf')
    assert task_id == task_id
    match = filesystem_regex.match(pattern)
    assert match is not None
    assert match.group(0) == pattern


@pytest.mark.parametrize(('architecture', 'architectures', 'download_filesystem'), [
    ('x86_64', None, True),
    (None, ['x86_64'], False),
    ('x86_64', ['x86_64', 'aarch64'], False),
    (None, None, True),
])
def test_image_download(tmpdir, docker_tasker, architecture, architectures, download_filesystem,
                        reactor_config_map):
    if MOCK:
        mock_docker()

    workflow = mock_workflow(tmpdir)
    mock_koji_session(download_filesystem=download_filesystem)
    mock_image_build_file(str(tmpdir))

    if reactor_config_map:
        make_and_store_reactor_config_map(workflow, {'root_url': '', 'auth': {}})

    runner = PreBuildPluginsRunner(
        docker_tasker,
        workflow,
        [{
            'name': PLUGIN_ADD_FILESYSTEM_KEY,
            'args': {
                'koji_hub': KOJI_HUB,
                'architecture': architecture,
                'architectures': architectures,
            }
        }]
    )

    results = runner.run()
    plugin_result = results[PLUGIN_ADD_FILESYSTEM_KEY]

    assert 'base-image-id' in plugin_result
    assert 'filesystem-koji-task-id' in plugin_result

    if download_filesystem:
        assert plugin_result['base-image-id'] == IMPORTED_IMAGE_ID
        assert plugin_result['filesystem-koji-task-id'] == FILESYSTEM_TASK_ID
    else:
        assert plugin_result['base-image-id'] is None
        assert plugin_result['filesystem-koji-task-id'] is None
