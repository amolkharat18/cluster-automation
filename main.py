# cluster automation
from dotenv import dotenv_values
import paramiko
import time

config = dotenv_values(".env")
username = config['USERNAME']
password = config['PASSWORD']


def get_ssh(server):
    # Create an SSH client object
    ssh = paramiko.SSHClient()
    # Automatically add the remote machine's SSH key to the known_hosts file
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server, username=username, password=password)
    return ssh


def is_hadoop_working(server):
    ssh = get_ssh(server)
    # Execute a bash command on the remote machine
    stdin, stdout, stderr = ssh.exec_command('hdfs dfsadmin -report')
    # Read the output from the command
    output = stdout.read().decode()
    # print(output)
    # Close the SSH connection
    ssh.close()
    return 'Configured Capacity' in output


def is_apache_kafka_working(server):
    ssh = get_ssh(server)
    # Execute a bash command on the remote machine
    stdin, stdout, stderr = ssh.exec_command('kafka-topics.sh --list --zookeeper localhost:2181')

    # Read the output from the command
    output = stdout.read().decode()
    topics = output.splitlines()
    print(len(topics), ' topics')

    # Close the SSH connection
    ssh.close()

    if output:
        return True
    else:
        return False


def is_confluent_kafka_working(server):
    ssh = get_ssh(server)
    # Execute a bash command on the remote machine
    stdin, stdout, stderr = ssh.exec_command('kafka-topics --list --zookeeper localhost:2181')

    # Read the output from the command
    output = stdout.read().decode()
    topics = output.splitlines()
    print(len(topics), ' topics')

    # Close the SSH connection
    ssh.close()

    if output:
        return True
    else:
        return False


def restart_hadoop(server):
    print(f'restart {server} start...')
    ssh = get_ssh(server)
    # Execute a bash command on the remote machine
    ssh.exec_command('stop-yarn.sh')
    time.sleep(5.0)
    ssh.exec_command('stop-dfs.sh')
    time.sleep(5.0)
    ssh.exec_command('mr-jobhistory-daemon.sh stop historyserver')
    time.sleep(5.0)
    ssh.exec_command('stop-slaves.sh')
    time.sleep(5.0)
    ssh.exec_command('stop-master.sh')
    time.sleep(5.0)

    ssh.exec_command('start-dfs.sh')
    time.sleep(5.0)
    ssh.exec_command('start-yarn.sh')
    time.sleep(5.0)
    ssh.exec_command('mr-jobhistory-daemon.sh start historyserver')
    time.sleep(5.0)
    ssh.exec_command('start-master.sh')
    time.sleep(5.0)
    ssh.exec_command('start-slaves.sh')
    ssh.close()
    print(f'restart {server} end...')


def restart_confluent_kafka(server):
    print(f'restart confluent kafka: {server} start...')
    ssh = get_ssh(server)
    # Execute a bash command on the remote machine
    path = "/home/projadm/Installations/confluent"

    cmd1 = f"{path}/bin/ksql-server-stop"
    ssh.exec_command(cmd1)
    time.sleep(2.0)

    cmd2 = f"{path}/bin/schema-registry-stop"
    ssh.exec_command(cmd2)
    time.sleep(2.0)

    cmd3 = f"{path}/bin/kafka-server-stop"
    ssh.exec_command(cmd3)
    time.sleep(2.0)

    cmd4 = f"{path}/bin/zookeeper-server-stop"
    ssh.exec_command(cmd4)
    time.sleep(2.0)

    cmd5 = f"{path}/bin/zookeeper-server-start -daemon {path}/etc/kafka/zookeeper.properties"
    ssh.exec_command(cmd5)
    time.sleep(5.0)

    cmd6 = f"{path}/bin/kafka-server-start -daemon {path}/etc/kafka/server.properties"
    ssh.exec_command(cmd6)
    time.sleep(5.0)

    cmd7 = f"{path}/bin/schema-registry-start -daemon {path}/etc/schema-registry/schema-registry.properties"
    ssh.exec_command(cmd7)
    time.sleep(5.0)

    cmd8 = f"{path}/bin/ksql-server-start -daemon {path}/etc/ksqldb/ksql-server.properties"
    ssh.exec_command(cmd8)
    time.sleep(5.0)

    ssh.close()
    print(f'restart confluent kafka: {server} end...')


def restart_apache_kafka(server):
    print(f'restart apache kafka: {server} start...')
    ssh = get_ssh(server)
    path = "/home/projadm/Installations/Kafka/kafka"

    cmd1 = f"{path}/bin/kafka-server-stop.sh"
    ssh.exec_command(cmd1)
    time.sleep(3.0)

    cmd2 = f"{path}/bin/zookeeper-server-stop.sh"
    ssh.exec_command(cmd2)
    time.sleep(3.0)

    # starting zookeeper service
    cmd3 = f"{path}/bin/zookeeper-server-start.sh -daemon {path}/config/zookeeper.properties"
    ssh.exec_command(cmd3)
    time.sleep(5.0)

    # Starting Brokers (server - 9092, server1 - 9093) as daemons
    cmd4 = f"{path}/bin/kafka-server-start.sh -daemon {path}/config/server.properties"
    ssh.exec_command(cmd4)
    time.sleep(5.0)

    cmd5 = f"{path}/bin/kafka-server-start.sh -daemon {path}/config/server1.properties"
    ssh.exec_command(cmd5)
    time.sleep(5.0)

    ssh.close()
    print(f'restart apache kafka: {server} end...')


def reset_password(server, user, pwd):
    ssh = get_ssh(server)
    ssh.get_transport()
    cmd1 = f'echo {user}:"{pwd}" | sudo chpasswd'
    print(cmd1)
    stdin, stdout, stderr = ssh.exec_command(command=cmd1, get_pty=True)
    stdin.write(f"{password}\n")
    stdin.flush()
    ssh.close()
    print(f'password reset done...')


if __name__ == '__main__':
    print('Cluster Automation...')
    # print(is_hadoop_working('server'))
    # print(is_apache_kafka_working('server'))
    # print(is_confluent_kafka_working('server'))
    # restart_hadoop('server')
    # restart_confluent_kafka('server')
    # restart_apache_kafka('server')
    #reset_password('server', 'user', 'password')
