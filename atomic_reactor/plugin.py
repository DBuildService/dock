"""
Copyright (c) 2015-2022 Red Hat, Inc
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.


definition of plugin system

plugins are supposed to be run when image is built and we need to extract some information
"""
import copy
import logging
import os
import sys
import traceback
import importlib
import inspect
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, TYPE_CHECKING, List, Optional

from atomic_reactor.util import exception_message

if TYPE_CHECKING:
    from atomic_reactor.inner import DockerBuildWorkflow

MODULE_EXTENSIONS = ('.py', '.pyc', '.pyo')
logger = logging.getLogger(__name__)


@dataclass
class PluginExecutionInfo:
    plugin_name: str
    plugin_class: "Plugin"
    conf: Dict[str, Any]
    is_allowed_to_fail: bool


class PluginFailedException(Exception):
    """ There was an error during plugin execution """


class TaskCanceledException(Exception):
    """Task was canceled"""


class Plugin(ABC):
    """ abstract plugin class """

    # by default, if plugin fails (raises exc), execution continues
    is_allowed_to_fail = True

    def __init__(self, workflow: "DockerBuildWorkflow", *args, **kwargs):
        """
        constructor

        :param workflow: DockerBuildWorkflow instance
        :param args: arguments from user input
        :param kwargs: keyword arguments from user input
        """
        self.workflow = workflow
        self.log = logging.getLogger("atomic_reactor.plugins." + self.key)
        self.args = args
        self.kwargs = kwargs

    @property
    @abstractmethod
    def key(self) -> str:
        """Unique plugin identification

        Output of this plugin can be found in results specified with this key,
        same thing goes for input: use this key for providing input for this plugin

        In subclass it can be specified just as "key" attribute
        """

        # Implementation notes: because this must be defined in each plugin it's abstract
        # property. For easy implementation it's just instance property not a class
        # property (with py<=3.8 it requires metaprogramming).
        # For py>3.8:
        #  ```
        #  @classmethod
        #  @property
        #  def key(cls) -> str
        #  ```
        return "plugin"

    def __str__(self):
        return "%s" % self.key

    def __repr__(self):
        return "Plugin(key='%s')" % self.key

    @abstractmethod
    def run(self):
        """
        each plugin has to implement this method -- it is used to run the plugin actually

        response from a build plugin is kept and used in json result response like this:

          results[plugin.key] = plugin.run()

        input plugins should emit build json with this method
        """

    @staticmethod
    def args_from_user_params(user_params: dict, /) -> dict:
        """Get keyword arguments for this plugin based on values in user params.

        Plugin runners will set these automatically for all plugins.
        """
        return {}


# Built-in plugins
class SleepPlugin(Plugin):
    """
    Sleep for a specified number of seconds.

    This plugin is only intended to be used for debugging.
    """

    key = 'sleep'

    def __init__(self, workflow, seconds=60):
        self.seconds = seconds

    def run(self):
        time.sleep(self.seconds)


class PluginsRunner(object):

    def __init__(
            self,
            workflow: "DockerBuildWorkflow",
            plugins_conf: List[Dict[str, Any]],
            plugin_files: Optional[List[str]] = None,
            keep_going: bool = False,
            plugins_results: Optional[Dict[str, Any]] = None,
    ) -> None:
        """constructor

        :param plugins_conf: list of dicts, configuration for plugins,
            e.g. [{'name': 'plugin_a', 'required': True}, ...]
        :type plugins_conf: list[dict[str, any]]
        :param plugin_files: optional file paths from where to load plugins.
        :type plugin_files: list[str]
        :param bool keep_going: keep running next plugin even if error is
            raised from previous plugin.
        """
        self.workflow = workflow
        self.plugins_results = {} if plugins_results is None else plugins_results
        self.plugins_conf = plugins_conf or []
        self.plugin_files = plugin_files or []
        self.plugin_classes = self.load_plugins()
        self.available_plugins = self.get_available_plugins()
        self.keep_going = keep_going

    def load_plugins(self) -> Dict[str, Plugin]:
        """
        load all available plugins

        :return: dict, bindings for plugins of the plugin_class_name class
        """
        # imp.findmodule('atomic_reactor') doesn't work
        plugins_dir = os.path.join(os.path.dirname(__file__), 'plugins')
        logger.debug("loading plugins from dir '%s'", plugins_dir)
        files = [os.path.join(plugins_dir, f)
                 for f in os.listdir(plugins_dir)
                 if f.endswith(".py")]
        if self.plugin_files:
            logger.debug("loading additional plugins from files '%s'", self.plugin_files)
            files += self.plugin_files
        plugin_classes = {}
        for f in files:
            module_name = os.path.basename(f).rsplit('.', 1)[0]
            # Do not reload plugins
            if module_name in sys.modules:
                f_module = sys.modules[module_name]
            else:
                try:
                    spec = importlib.util.spec_from_file_location(module_name, f)
                    f_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(f_module)
                except (IOError, OSError, ImportError, SyntaxError) as ex:
                    logger.warning("can't load module '%s': %s", f, ex)
                    continue
            for name in dir(f_module):
                binding = getattr(f_module, name)
                try:
                    # if you try to compare binding and Plugin, python won't match them
                    # if you call this script directly b/c:
                    # ! <class 'plugins.plugin_rpmqa.PostBuildRPMqaPlugin'> <= <class
                    # '__main__.Plugin'>
                    # but
                    # <class 'plugins.plugin_rpmqa.PostBuildRPMqaPlugin'> <= <class
                    # 'atomic_reactor.plugin.Plugin'>
                    is_sub = issubclass(binding, Plugin)
                except TypeError:
                    is_sub = False
                if binding and is_sub and Plugin.__name__ != binding.__name__:
                    plugin_classes[binding.key] = binding
        return plugin_classes

    def get_available_plugins(self):
        """
        check requested plugins availability
        and handle missing plugins

        :return: list of plugin execution info
        :rtype: list[PluginExecutionInfo]
        """
        available_plugins = []
        for plugin_request in self.plugins_conf:
            plugin_name = plugin_request['name']
            try:
                plugin_class = self.plugin_classes[plugin_name]
            except KeyError as e:
                if plugin_request.get('required', True):
                    msg = ("no such plugin: '%s', did you set "
                           "the correct plugin type?") % plugin_name
                    exc = PluginFailedException(msg)
                    self.on_plugin_failed(plugin_name, exc)
                    logger.error(msg)
                    raise exc from e
                else:
                    # This plugin is marked as not being required
                    logger.warning("plugin '%s' requested but not available",
                                   plugin_name)
                    continue
            plugin_is_allowed_to_fail = plugin_request.get('is_allowed_to_fail',
                                                           getattr(plugin_class,
                                                                   "is_allowed_to_fail", True))
            plugin_conf = plugin_request.get("args", {})
            available_plugins.append(
                PluginExecutionInfo(
                    plugin_name=plugin_name,
                    plugin_class=plugin_class,
                    conf=plugin_conf,
                    is_allowed_to_fail=plugin_is_allowed_to_fail
                )
            )
        return available_plugins

    def on_plugin_failed(
            self,
            plugin: Optional[str] = None,
            exception: Optional[Exception] = None,
    ):
        if plugin and exception:
            self.workflow.data.plugins_errors[plugin] = str(exception)

    def save_plugin_timestamp(self, name: str, timestamp: datetime) -> None:
        self.workflow.data.plugins_timestamps[name] = timestamp.isoformat()

    def save_plugin_duration(self, name: str, duration: float) -> None:
        self.workflow.data.plugins_durations[name] = duration

    def _translate_special_values(self, obj_to_translate):
        """
        you may want to write plugins for values which are not known before build:
        e.g. id of built image, base image name,... this method will therefore
        translate some reserved values to the runtime values
        """
        translation_dict = {
            'BUILD_DOCKERFILE_PATH': self.workflow.source.dockerfile_path,
            'BUILD_SOURCE_PATH': self.workflow.source.path,
        }

        if isinstance(obj_to_translate, dict):
            # Recurse into dicts
            translated_dict = copy.deepcopy(obj_to_translate)
            for key, value in obj_to_translate.items():
                translated_dict[key] = self._translate_special_values(value)

            return translated_dict
        elif isinstance(obj_to_translate, list):
            # Iterate over lists
            return [self._translate_special_values(elem)
                    for elem in obj_to_translate]
        else:
            return translation_dict.get(obj_to_translate, obj_to_translate)

    def _remove_unknown_args(self, plugin_class, plugin_conf):
        sig = inspect.getfullargspec(plugin_class.__init__)  # pylint: disable=no-member
        kwargs = sig.varkw

        # Constructor defines **kwargs, it'll take any parameter
        if kwargs:
            return plugin_conf

        args = set(sig.args)
        known_plugin_conf = {}
        for key, value in plugin_conf.items():
            if key not in args:
                logger.warning(
                    '%s constructor does not take %s=%s parameter, ignoring it',
                    plugin_class.__name__, key, value)
                continue
            known_plugin_conf[key] = value

        return known_plugin_conf

    def create_instance_from_plugin(self, plugin_class, plugin_conf: Dict[str, Any]):
        plugin_conf = self._remove_unknown_args(plugin_class, plugin_conf)
        plugin_conf.update(plugin_class.args_from_user_params(self.workflow.user_params))
        plugin_conf = self._translate_special_values(plugin_conf)
        logger.info("running plugin instance with args: '%s'", plugin_conf)
        plugin_instance = plugin_class(self.workflow, **plugin_conf)
        return plugin_instance

    @contextmanager
    def _execution_timer(self, exec_info: PluginExecutionInfo) -> Generator:
        logger.debug("running plugin '%s'", exec_info.plugin_name)
        start_time = datetime.now()
        plugin_key = exec_info.plugin_class.key
        self.save_plugin_timestamp(plugin_key, start_time)
        try:
            yield
        finally:
            try:
                finish_time = datetime.now()
                duration = finish_time - start_time
                seconds = duration.total_seconds()
                logger.debug("plugin '%s' finished in %ds", exec_info.plugin_name, seconds)
                self.save_plugin_duration(plugin_key, seconds)
            except Exception:
                logger.exception("failed to save plugin duration")

    def run(self):
        """Run all requested plugins."""
        failed_msgs: List[str] = []
        available_plugins = self.available_plugins
        for plugin in available_plugins:
            plugin_key = plugin.plugin_class.key
            try:
                plugin_instance = self.create_instance_from_plugin(
                    plugin.plugin_class, plugin.conf
                )
                with self._execution_timer(plugin):
                    self.plugins_results[plugin_key] = plugin_instance.run()
            except Exception as ex:
                logger.debug(traceback.format_exc())

                if not plugin.is_allowed_to_fail:
                    self.on_plugin_failed(plugin.plugin_class.key, ex)

                msg = f"plugin '{plugin_key}' raised an exception: {exception_message(ex)}"
                if plugin.is_allowed_to_fail or self.keep_going:
                    logger.warning(msg)
                    logger.info("error is not fatal, continuing...")
                    if not plugin.is_allowed_to_fail:
                        failed_msgs.append(msg)
                else:
                    logger.error(msg)
                    raise PluginFailedException(msg) from ex

        if len(failed_msgs) == 1:
            raise PluginFailedException(failed_msgs[0])
        elif len(failed_msgs) > 1:
            raise PluginFailedException(
                f"Multiple plugins raised an exception: {str(failed_msgs)}"
            )

        return self.plugins_results
