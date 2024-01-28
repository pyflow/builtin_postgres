import copy
import importlib.resources
import tempfile
import shutil
import os
import platform
import subprocess
from zipfile import ZipFile


class BuiltinPostgres:
    def __init__(self, data_dir=None, port=15432, bind='127.0.0.1'):
        self.postgres_process = None
        self.temp_root = None
        self.data_dir = None
        self.port = port
        self.bind = bind

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
            return 'pg_ctl.exe'
        else:
            return 'pg_ctl'

    def get_bin_zip_name(self):
        system = platform.system().lower()
        machine = platform.machine().lower()
        return "postgres-{}-{}.zip".format(system, machine)


    def prepare_postgres_bin(self):
        self.temp_root = tempfile.mkdtemp(prefix="builtin_postgres")
        root = importlib.resources.files('builtin_postgres')
        target = root.joinpath('bin', self.get_platform_name(),  self.get_bin_zip_name())

        with importlib.resources.as_file(target) as taret_path:
            name = os.path.splitext(target.name)[0]
            postgres_bin = os.path.join(self.temp_root, name, "bin", self.get_bin_name())
            with ZipFile(taret_path) as pgzip:
                pgzip.extractall(self.temp_root)

        return postgres_bin

    def encode_parameters(self):
        encoded = []
        encoded.append("-p {}".format(self.port))
        return '"{}"'.format(" ".join(encoded))

    def start(self):
        if self.postgres_process != None:
            return
        postgres_bin = self.prepare_postgres_bin()
        start_args = [postgres_bin, "start", "-w", "-D", self.data_dir, "-o", self.ecncode_paramters()]
        self.postgres_process = subprocess.Popen(start_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
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
