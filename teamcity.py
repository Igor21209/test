from subprocess import Popen, PIPE
import subprocess
import yaml
from yaml.loader import SafeLoader
import re
import sys


class Teamcity:
    def __init__(self, user, host, target_dir, path_to_ssh_priv_key, path_to_yaml, path_to_sqlplus, oracle_host, oracle_db, schema):
        self.user = user
        self.host = host
        self.target_dir = target_dir
        self.path_to_ssh_priv_key = path_to_ssh_priv_key
        self.path_to_yaml = path_to_yaml
        self.path_to_sqlplus = path_to_sqlplus
        self.oracle_host = oracle_host
        self.oracle_db = oracle_db
        self.schema = schema
        self.password = self.get_env_variable("echo $pass") 

    def runSqlQuery(self, sqlCommand):
        session = Popen([f'{self.path_to_sqlplus}',
                         f'{self.schema}/{self.password}@//{self.oracle_host}:1521/{self.oracle_db}'], stdin=PIPE, stdout=PIPE,
                        stderr=PIPE)
        session.stdin.write(sqlCommand)
        if session.communicate():
            unknown_command = re.search('unknown command', session.communicate()[0].decode('UTF-8'))
            if session.returncode != 0:
                pass
                #sys.exit(f'Error while executing sql code in file {sqlCommand}')
            if unknown_command:
                pass
                #sys.exit(f'Error while executing sql code in file {sqlCommand}')
        return session.communicate()

    def get_env_variable(self, command):
        process = Popen(
            args=command,
            stdout=PIPE,
            shell=True
        )
        return process.communicate()[0].decode('UTF-8').strip()

    def yaml_parser(self, path):
        with open(f'{path}', 'r') as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data

    def execute_files(self, data_dict):
        sql = data_dict.get('sql')
        sas = data_dict.get('sas')
        if sql:
            for q in sql:
                q = f'@{q}'
                byte = bytes(q, 'UTF-8')
                tes = self.runSqlQuery(byte)
                print(tes[0].decode('UTF-8'))
                print(self.get_env_variable("echo $pass"))
        if sas:
            for s in sas:
                self.ssh_copy(s, self.target_dir)

    def ssh_copy(self, sourse, target):
        dirs = re.split('/', sourse)
        create_dirs = ''
        for i in dirs:
            if i == dirs[-1]:
                break
            create_dirs = create_dirs + i + '/'
        dirs = subprocess.run(
            ['ssh', '-i', f'{self.path_to_ssh_priv_key}', f'{self.user}@{self.host}', 'mkdir', '-p', f'{target + create_dirs}'])
        if dirs.returncode != 0:
            sys.exit('Error while making directories on the server')
        files = subprocess.run(
            ['scp', '-i', f'{self.path_to_ssh_priv_key}', '-r', f'{sourse}', f'{self.user}@{self.host}:{target + create_dirs}'])
        if files.returncode != 0:
            sys.exit('Error while copying file on the server')
        create_dirs = ''

    def start(self):
        data = self.yaml_parser(self.path_to_yaml)
        self.execute_files(data)










