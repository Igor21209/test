from subprocess import Popen, PIPE
import subprocess
import yaml
from yaml.loader import SafeLoader
import re
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
import os


@dataclass
class Commit:
    commit: str = None
    date: datetime = None
    branch: str = None


class Teamcity:
    def __init__(self, user, host, target_dir, path_to_ssh_priv_key, path_to_yaml, path_to_sqlplus, oracle_host, oracle_db, oracle_user, oracle_port):
        self.user = user
        self.host = host
        self.target_dir = target_dir
        self.path_to_ssh_priv_key = path_to_ssh_priv_key
        self.path_to_yaml = path_to_yaml
        self.path_to_sqlplus = path_to_sqlplus
        self.oracle_host = oracle_host
        self.oracle_db = oracle_db
        self.oracle_user = oracle_user
        self.oracle_port = oracle_port

    def runSqlQuery(self, sqlCommand, sqlFile=None):
        if sqlCommand:
            with tempfile.NamedTemporaryFile('w+', encoding='UTF-8', suffix='.sql', dir='/tmp') as fp:
                fp.write(sqlCommand)
                fp.flush()
                sql = bytes(f"@{fp.name}", 'UTF-8')
                session = Popen([f'{self.path_to_sqlplus}', '-S',
                                 f'{self.oracle_user}/{os.environ.get("PASS")}@//{self.oracle_host}:{self.oracle_port}/{self.oracle_db}'],
                                stdin=PIPE, stdout=PIPE,
                                stderr=PIPE)
                session.stdin.write(sql)
                if session.communicate():
                    unknown_command = re.search('unknown command', session.communicate()[0].decode('UTF-8'))
                    if session.returncode != 0:
                        sys.exit(f'Error while executing sql code in file {sqlCommand}')
                    if unknown_command:
                        sys.exit(f'Error while executing sql code in file {sqlCommand}')
                return session.communicate()
        else:
            sql = bytes(f"@{sqlFile}", 'UTF-8')
        session = Popen([f'{self.path_to_sqlplus}', '-S',
                         f'{self.oracle_user}/{os.environ.get("PASS")}@//{self.oracle_host}:{self.oracle_port}/{self.oracle_db}'], stdin=PIPE, stdout=PIPE,
                        stderr=PIPE)
        session.stdin.write(sql)
        if session.communicate():
            unknown_command = re.search('unknown command', session.communicate()[0].decode('UTF-8'))
            if session.returncode != 0:
                sys.exit(f'Error while executing sql code in file {sqlCommand}')
            if unknown_command:
                sys.exit(f'Error while executing sql code in file {sqlCommand}')
        return session.communicate()

    def yaml_parser(self, path):
        with open(f'{path}', 'r') as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data

    '''
    Метод предназначен для восстановления порядка установки патчей в соответствии с порядком, указанным в файле deploy_order.yml.
    Он необходим, т.к. бд возвращает случайный порядок патчей для установки.
    set(pathes_for_install) - set(list_of_installed_pathes_from_db) не получится использовать, т.к. порядок в таком случае не сохраняется.
    '''
    def check_patches(self, pathes_for_install, list_of_installed_pathes_from_db):
        sp = set(list_of_installed_pathes_from_db)
        pathes_for_install = [p for p in pathes_for_install if p in sp]
        return pathes_for_install

    '''
    Метод необходим для проверки соответствия количества патчей, предполагаемых установке, и правильность порядка их следования.
    Собранный список объектов класса Commit в методе git сверяется со списком патчей для устнавки, собранным в соответствии с 
    порядком, указанным в файле deploy_order.yml
    '''
    def check_incorrect_order(self, commits_array, branch_array):
        result_compare_order = False
        commits_list = [commit.commit for commit in commits_array]
        if not len(commits_list) == len(branch_array):
            result_compare_order = True
        return result_compare_order

    def get_current_branch(self):
        current_branch = self.run_shell_command('git branch --show-current').strip()
        return current_branch

    def log_patch_db_success(self, patch):
        add_to_install_patches = f"""whenever sqlerror exit sql.sqlcode
MERGE INTO PATCH_STATUS USING DUAL ON (PATCH_NAME = '{patch}')
WHEN NOT MATCHED THEN INSERT (PATCH_NAME, INSTALL_DATE, STATUS)
VALUES('{patch}', current_timestamp, 'SUCCESS')
WHEN MATCHED THEN UPDATE SET INSTALL_DATE=current_timestamp, STATUS='SUCCESS';
exit;"""
        self.runSqlQuery(add_to_install_patches)


    def execute_files(self, patches_from_deploy_order):
        patches = patches_from_deploy_order.get('patch')
        patches_for_install = self.get_patches_for_install(patches)
        if len(patches_for_install) == 0:
            sys.exit(f'Nothing to install')
        patches_for_install_order = self.check_patches(patches, patches_for_install)
        list_of_commit_objects = self.git(patches_for_install)
        check = self.check_incorrect_order(list_of_commit_objects, patches_for_install_order)
        if not check:
            for patch in list_of_commit_objects:
                pars = f'Patches/{patch.branch}/deploy.yml'
                data = self.yaml_parser(pars)
                sql = data.get('sql')
                sas = data.get('sas')
                if sql:
                    if not (len(patches_for_install) == 1 and self.get_current_branch() == patches_for_install[0]):
                        for q in sql:
                            query = self.get_commit_version(q, patch.commit)
                            self.runSqlQuery(query)
                    else:
                        for q in sql:
                            self.runSqlQuery(query)
                if sas:
                    for s in sas:
                        self.ssh_copy(s, self.target_dir)
                self.log_patch_db_success(patch.branch)
        else:
            sys.exit(f"Patches order does not match commits order")

    def ssh_copy(self, sourse, target):
        dirs = re.split('/', sourse)
        create_dirs = ''
        for i in dirs:
            if i == dirs[-1]:
                break
            create_dirs = create_dirs + i + '/'
        create = re.search('SAS/(.+)', create_dirs)
        if create:
            dir_for_create = create.group(1)
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
        sql_command = sql_exec.communicate()[0].decode('UTF-8')
        return sql_command

    '''
    Метод необходим для поиска версии sql скрипта из ветки для которой был выполнен merge в ветку release
    1. Находим все merge в ветку release командой git rev-list --merges HEAD ^Jira_X
    2. Через команду git show <хэш коммита> находим нужную версию коммита
    Результатом работы метода является отсортированный по дате список объектов класса Commit, которые содержат в себе:
    - нужный хэш коммита, по которому потом будут применяться sql скрипты (метод get_commit_version)
    - дата этого коммита
    - название ветки с патчом
    '''
    def git(self, patches):
        commit_list = []
        for patch_name in patches:
            rev_list = f'git rev-list --merges HEAD ^{patch_name}'
            commits = self.run_shell_command(rev_list)
            list_of_commits = re.findall('(.+)\n', commits)
            for commit in list_of_commits:
                branch = f'git show {commit}'
                get_branch = self.run_shell_command(branch)
                date = re.search('Date: (.+)', get_branch).group(1).strip()
                branch_name = re.search('\{\%(.+)\%\}', get_branch).group(1)
                if branch_name == patch_name:
                    commit_list.append(Commit(commit, date, branch_name))
        commit_list.sort(reverse=False, key=lambda comm: comm.date)
        return commit_list

    def get_patches_for_install(self, patches):
        patches_for_install = []
        query_1 = """whenever sqlerror exit sql.sqlcode
CREATE OR REPLACE TYPE arr_patch_type IS TABLE OF VARCHAR2(32);
/
exit;"""
        self.runSqlQuery(query_1)
        deploy_order = str(patches).replace('[', '(').replace(']', ')').strip()
        query_2 = f"""SET SERVEROUTPUT ON
whenever sqlerror exit sql.sqlcode
DECLARE
  all_patches_list arr_patch_type := arr_patch_type{deploy_order};
  uninstalled_patches arr_patch_type := arr_patch_type();
  installed_patches arr_patch_type := arr_patch_type();
BEGIN
  SELECT PATCH_NAME BULK COLLECT INTO installed_patches FROM PATCH_STATUS
  WHERE PATCH_NAME IN (select * from table(all_patches_list));
  uninstalled_patches := all_patches_list MULTISET EXCEPT installed_patches;
  DBMS_OUTPUT.PUT_LINE('START_RES');
  FOR i IN 1..uninstalled_patches.COUNT LOOP
    DBMS_OUTPUT.PUT_LINE(uninstalled_patches(i));
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('FINISH_RES');
END;
/
exit;"""
        test = self.runSqlQuery(query_2)
        all_patches = re.search('START_RES\n(.+)\nFINISH_RES', test[0].decode('UTF-8'), re.S)
        patches_for_install = all_patches.group(1).split('\n')
        return patches_for_install

    def start(self):
        data = self.yaml_parser(self.path_to_yaml)
        self.execute_files(data)
