from subprocess import Popen, PIPE
import subprocess
import yaml
from yaml.loader import SafeLoader
import re
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Commit:
    commit: str = None
    date: datetime = None
    branch: str = None


class Teamcity:
    def __init__(self, user, host, target_dir, path_to_ssh_priv_key, path_to_yaml, path_to_sqlplus, oracle_host, oracle_db, oracle_user):
        self.user = user
        self.host = host
        self.target_dir = target_dir
        self.path_to_ssh_priv_key = path_to_ssh_priv_key
        self.path_to_yaml = path_to_yaml
        self.path_to_sqlplus = path_to_sqlplus
        self.oracle_host = oracle_host
        self.oracle_db = oracle_db
        self.oracle_user = oracle_user

    def runSqlQuery(self, sqlCommand):
        session = Popen([f'{self.path_to_sqlplus}', '-S',
                         f'{self.oracle_user}/{self.get_env_variable("echo $PASS")}@//{self.oracle_host}:1521/{self.oracle_db}'], stdin=PIPE, stdout=PIPE,
                        stderr=PIPE)
        session.stdin.write(sqlCommand)
        if session.communicate():
            unknown_command = re.search('unknown command', session.communicate()[0].decode('UTF-8'))
            if session.returncode != 0:
                sys.exit(f'Error while executing sql code in file {sqlCommand}')
            if unknown_command:
                sys.exit(f'Error while executing sql code in file {sqlCommand}')
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

    def check_patches(self, pathes_for_install, list_of_installed_pathes_from_db):
        index_scan = 0
        while index_scan < len(pathes_for_install):
            if pathes_for_install[index_scan] not in (list_of_installed_pathes_from_db):
                pathes_for_install.pop(index_scan)
            else:
                index_scan += 1
        return pathes_for_install

    def check_incorrect_order(self, commits_array, branch_array):
        patch_index = 0
        result_compare_order = False
        if len(commits_array) < len(branch_array):
            return True
        while branch_array[0] != commits_array[patch_index].branch and patch_index < len(commits_array):
            patch_index += 1
        for branch in branch_array:
            if patch_index >= len(commits_array):
                result_compare_order = True
                return result_compare_order
            if branch != commits_array[patch_index].branch:
                result_compare_order = True
                return result_compare_order
            patch_index += 1
        return result_compare_order

    def execute_files(self, patches):
        patches_1 = patches.get('patch')
        patches_for_install = self.get_patches_for_install(patches_1)
        patches_for_install_order = self.check_patches(patches_1, patches_for_install)
        list_of_commit_objects = self.git(patches_for_install)
        check = self.check_incorrect_order(list_of_commit_objects, patches_for_install_order)
        if check:
            for patch in list_of_commit_objects:
                pars = f'Patches/{patch.branch}/deploy.yml'
                data = self.yaml_parser(pars)
                sql = data.get('sql')
                sas = data.get('sas')
                if sql:
                    for q in sql:
                        query = self.get_commit_version(q, patch.commit)
                        self.runSqlQuery(query)
                if sas:
                    for s in sas:
                        self.ssh_copy(s, self.target_dir)
        else:
            sys.exit(f'Some problem with patch')

    def ssh_copy(self, sourse, target):
        dirs = re.split('/', sourse)
        create_dirs = ''
        for i in dirs:
            if i == dirs[-1]:
                break
            create_dirs = create_dirs + i + '/'
        create = re.search('(SAS/).+', create_dirs)
        if create:
            dir_for_create = create.group(0)[4:]
            dirs = subprocess.run(
                ['ssh', '-i', f'{self.path_to_ssh_priv_key}', f'{self.user}@{self.host}', 'mkdir', '-p',
                 f'{target + dir_for_create}'])
            if dirs.returncode != 0:
                sys.exit('Error while making directories on the server')
            files = subprocess.run(
                ['scp', '-i', f'{self.path_to_ssh_priv_key}', '-r', f'{sourse}',
                 f'{self.user}@{self.host}:{target + dir_for_create}'])
            if files.returncode != 0:
                sys.exit('Error while copying file on the server')
        else:
            files = subprocess.run(
                ['scp', '-i', f'{self.path_to_ssh_priv_key}', '-r', f'{sourse}',
                 f'{self.user}@{self.host}:{target}'])
            if files.returncode != 0:
                sys.exit('Error while copying file on the server')
        create_dirs = ''

    def run_shell_command(self, command):
        process = Popen(args=command, stdout=PIPE, shell=True)
        return process.communicate()[0].decode('UTF-8')

    def get_commit_version(self, sql_path, commit):
        command_1 = f'git show {commit}:./{sql_path}'
        sql_exec = Popen(args=command_1,
            stdout=PIPE,
            shell=True)
        sql_command = sql_exec.communicate()[0]
        return sql_command

    def sort(self, date):
        return date.date

    def git(self, patches):
        commit_list = []
        for patch_name in patches:
            rev_list = f'git rev-list --merges HEAD ^{patch_name}'
            commits = self.run_shell_command(rev_list)
            list_of_commits = re.findall('(.+)\n', commits)
            for commit in list_of_commits:
                branch = f'git show {commit}'
                get_branch = self.run_shell_command(branch)
                branch_1 = re.search('Merge: .+ (.+)', get_branch).group(1)
                get_branch_1 = self.run_shell_command(f'git show  {branch_1}')
                commit_version = re.search('commit (.+)', get_branch_1).group(1)
                date = re.search('Date: (.+)', get_branch_1).group(1).strip()
                name_of_branch = self.run_shell_command(f'git name-rev {branch_1}')
                branch_name = re.search('.+ (.+)', name_of_branch).group(1)
                if branch_name == patch_name:
                    commit_list.append(Commit(commit_version, date, branch_name))
        commit_list.sort(reverse=False, key=self.sort)
        print(commit_list)
        return commit_list

    def get_patches_for_install(self, patches):
        patches_for_install = []
        query_1 = "whenever sqlerror exit sql.sqlcode\
        \nCREATE OR REPLACE TYPE arr_patch_type IS TABLE OF VARCHAR2(32);\
        \n/\
        \nexit;"
        with tempfile.NamedTemporaryFile('w+', encoding='UTF-8', suffix='.sql', dir='/tmp') as fp:
            fp.write(query_1)
            fp.seek(0)
            self.runSqlQuery(bytes(f"@{fp.name}", 'UTF-8'))
        deploy_order = str(patches).replace('[', '(').replace(']', ')').strip()
        query_2 = f"SET SERVEROUTPUT ON\
        \nwhenever sqlerror exit sql.sqlcode\
        \nDECLARE\
        \nall_patches_list arr_patch_type := arr_patch_type{deploy_order};\
        \nuninstalled_patches arr_patch_type := arr_patch_type();\
        \ninstalled_patches arr_patch_type := arr_patch_type();\
        \nBEGIN\
        \nSELECT PATCH_NAME BULK COLLECT INTO installed_patches FROM PATCH_STATUS\
        \nWHERE PATCH_NAME IN (select * from table(all_patches_list));\
        \nuninstalled_patches := all_patches_list MULTISET EXCEPT installed_patches;\
        \nFOR i IN 1..uninstalled_patches.COUNT LOOP\
        \nDBMS_OUTPUT.PUT_LINE(uninstalled_patches(i));\
        \nEND LOOP;\
        \nEND;\
        \n/\
        \nexit;"
        with tempfile.NamedTemporaryFile('w+', encoding='UTF-8', suffix='.sql', dir='/tmp') as fp:
            fp.write(query_2)
            fp.seek(0)
            test = self.runSqlQuery(bytes(f"@{fp.name}", 'UTF-8'))
            patches_for_install = re.findall('(.+)\n', test[0].decode('UTF-8'))
            patches_for_install.pop(-1)
        return patches_for_install

    def start(self):
        data = self.yaml_parser(self.path_to_yaml)
        self.execute_files(data)









