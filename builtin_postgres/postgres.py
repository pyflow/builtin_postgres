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
        self.temp_root = None
        self.data_dir = None
        self.port = port
        self.bind = bind
        self.started = False

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
        if self.started:
            return
        postgres_bin = self.prepare_postgres_bin()
        start_args = [postgres_bin, "start", "-w", "-D", self.data_dir, "-o", self.ecncode_paramters()]
        postgres_process = subprocess.Popen(start_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        try:
            outs, errs = postgres_process.communicate(timeout=12)
        except subprocess.TimeoutExpired:
            raise Exception('BuiltPosgres Start Failed.')
        self.started = True


    def stop(self):
        postgres_bin = self.prepare_postgres_bin()
        stop_args = [postgres_bin, "start", "-w", "-D", self.data_dir, "-o", self.ecncode_paramters()]
        process = subprocess.Popen(stop_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        try:
            outs, errs = process.communicate(timeout=12)
        except subprocess.TimeoutExpired:
            raise Exception('BuiltPosgres Stop Failed.')

        if self.temp_root != None:
            # clear temp directory content
            try:
                shutil.rmtree(self.temp_root)
                self.temp_root = None
            except Exception as e:
                # ignore
                pass
