import copy
import importlib.resources
import tempfile
import shutil
import os
import platform
import subprocess

DEFAULT_CONF = {
}

QUOTED_KEYS = [
]

class BuiltinPostgres:
    def __init__(self, port=15432, bind='127.0.0.1'):
        self.postgres_process = None
        self.conf = copy.deepcopy(DEFAULT_CONF)
        self.temp_root = None
        self.set_conf('port', port)
        self.set_conf('bind', bind)

    def get_platform_name(self):
        system = platform.system().lower()
        machine = platform.machine().lower()
        arch = 'unknown'
        if machine in ['amd64']:
            arch = machine
        return '{}_{}'.format(system, arch)

    def get_bin_name(self):
        system = platform.system().lower()
        if system == 'windows':
            return 'postgres.exe'
        else:
            return 'postgres'

    def set_conf(self, key, value):
        pass


    def get_conf_content(self):
        pass


    def start(self):
        if self.postgres_process != None:
            return
        postgres_bin, postgres_confpath = self.prepare_postgres_bin()
        self.postgres_process = subprocess.Popen([postgres_bin, postgres_confpath], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        try:
            outs, errs = self.postgres_process.communicate(timeout=3)
            self.postgres_process.kill()
            self.postgres_process = None
            raise Exception('BuiltPosgres Start Failed.')
        except subprocess.TimeoutExpired:
            return


    def stop(self):
        if self.postgres_process != None:
            self.postgres_process.kill()
            self.postgres_process.wait(3)
            self.postgres_process = None

        if self.temp_root != None:
            # clear temp directory content
            try:
                shutil.rmtree(self.temp_root)
                self.temp_root = None
            except Exception as e:
                # ignore
                pass
