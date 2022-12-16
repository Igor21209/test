from sys import argv
from teamcity import Teamcity

python_path, user, host, target_dir, path_to_ssh_priv_key, path_to_yaml, path_to_sqlplus, oracle_host, oracle_db, oracle_user, oracle_port = argv

if __name__ == '__main__':
    test = Teamcity(user, host, target_dir, path_to_ssh_priv_key, path_to_yaml, path_to_sqlplus, oracle_host, oracle_db, oracle_user, oracle_port)
    test.start()
