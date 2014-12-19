DiMint_Overlord
===============
##Installation
DiMint 서버는 ZooKeeper, Overlord와 Node로 구성되어 있습니다.
ZooKeeper는 구글링을 통해 쉽게 설치하실 수 있습니다.
아래의 유저 메뉴얼은 로컬 머신에서 ZooKeeper를 single instance로 실행한다고 가정합니다.

ZooKeeper를 설치하시고, Overlord source file를 받아주시기 바랍니다.
```
$ git clone https://github.com/dimint/dimint_overlord.git
$ cd dimint_overlord
```
python3로 작성되었기 때문에 python3가 설치된 환경에서 실행해주시기 바랍니다.
권한 문제를 해결하기 위해 virtualenv 환경에서 실행하였습니다.
```
$ virtualenv -p /usr/bin/python3 myenv
$ source ./myenv/bin/activate
$ python setup.py install
```
설치가 완료되었습니다. 다음의 명령어로 virtualenv 환경을 빠져나오실 수 있습니다.
다시 virtualenv 환경으로 가고 싶으시면 virtualenv가 설정된 myenv/bin/activate를 실행하시면 됩니다.
```
$ deactivate
```
##Execution
virtualenv 환경으로 진입한다음에 dimint 또는 dimint_overlord 로 overlord server를 띄울 수 있습니다.
물론 실행이 다 끝나고 deactivate 명령어로 virtualenv를 끝낼 수 있습니다.
dimint 를 치면 도움말이 나옵니다.
```
$ source ./myenv/bin/activate
$ dimint

    dimint help
    dimint overlord help
    dimint overlord list
    dimint overlord start
    dimint overlord stop

$ dimint overlord help

    dimint overlord help
    dimint overlord list
    dimint overlord start
    dimint overlord stop

$ deactivate
```
(이후 튜토리얼에서는 virtualenv 실행/해제는 보이지 않습니다.)
dimint overlord list는 현재 실행중인 python, dimint 프로그램을 모두 보여줍니다.
(따라서 로컬에서 실행중인 Node도 함께 보일 수 있습니다.)
프로그램을 종료시킬 때 pid가 필요하기 때문에 이용하면 편할 수 있습니다.
```
$ dimint_overlord list
psutil.Process(pid=21777, name='dimint'), cmdline : ['/home/jsryu21/dimint_node/myenv/bin/python', '/home/jsryu21/dimint_node/myenv/bin/dimint', 'node', 'start']
psutil.Process(pid=22652, name='dimint'), cmdline : ['/home/jsryu21/dimint_overlord/myenv/bin/python', '/home/jsryu21/dimint_overlord/myenv/bin/dimint', 'overlord', 'list']
```
dimint overlord start는 기본 config 파일을 읽어서 overlord를 띄웁니다.
기본 config 파일은 dimint_overlord/dimint_overlord.config 입니다.
다른 config 파일을 사용하고 싶으시면 dimint overlord start --config_path=another_config_path 이렇게 해주시면 됩니다.
따로 config 파일을 만들지 않고 설정값을 넣어서 overlord를 띄우고 싶으시면 dimint overlord start --port_for_client=5556 --port_for_node=5557 --zookeeper_hosts=115.71.237.6:2181 --hash_range=10000 --max_slave_count=2 이렇게 해주시면 됩니다.
overlord를 중단하고 싶으시면 dimint overlord stop pid 와 같이 적어주시되, pid 부분에 죽이고 싶은 overlord의 pid를 적으면 됩니다.
```
$ dimint overlord start
Hello from parent 23129 23131
$ dimint overlord list
psutil.Process(pid=21777, name='dimint'), cmdline : ['/home/jsryu21/dimint_node/myenv/bin/python', '/home/jsryu21/dimint_node/myenv/bin/dimint', 'node', 'start']
psutil.Process(pid=23131, name='dimint'), cmdline : ['/home/jsryu21/dimint_overlord/myenv/bin/python', '/home/jsryu21/dimint_overlord/myenv/bin/dimint', 'overlord', 'start']
psutil.Process(pid=23154, name='dimint'), cmdline : ['/home/jsryu21/dimint_overlord/myenv/bin/python', '/home/jsryu21/dimint_overlord/myenv/bin/dimint', 'overlord', 'list']
$ dimint overlord stop 23131
$ dimint overlord start --config_path=dimint_overlord/dimint_overlord.config
Hello from parent 23201 23203
$ dimint overlord stop 23203
$ dimint overlord start --port_for_client=5556 --port_for_node=5557 --zookeeper_hosts=115.71.237.6:2181 --hash_range=10000 --max_slave_count=2
Hello from parent 23254 23256
$ dimint overlord stop 23256
$ dimint overlord list
psutil.Process(pid=21777, name='dimint'), cmdline : ['/home/jsryu21/dimint_node/myenv/bin/python', '/home/jsryu21/dimint_node/myenv/bin/dimint', 'node', 'start']
psutil.Process(pid=23292, name='dimint'), cmdline : ['/home/jsryu21/dimint_overlord/myenv/bin/python', '/home/jsryu21/dimint_overlord/myenv/bin/dimint', 'overlord', 'list']
```
다른 프로그램인 dimint_overlord는 dimint overlord 명령어의 줄임버전입니다.
--config_path로 옵션으로 다른 config를 넣어줄 수 있고, config_path 없이도 --port_for_client=5556 --port_for_node=5557 --zookeeper_hosts=127.0.0.1:2121 --hash_range=10000 --max_slave_count=2 이렇게 옵션을 주어 실행할 수 있습니다.
단, dimint 프로그램처럼 fork로 띄우는 식이 아니라 바로 띄우기 때문에 shell을 차지합니다.
따라서 ctrl+c를 통해 프로그램을 종료시킬 수 있습니다.
```
$ dimint_overlord
ip : 115.71.237.6
OverlordStateTask works
^CException ignored in: <module 'threading' from '/usr/local/lib/python3.4/threading.py'>
Traceback (most recent call last):
  File "/usr/local/lib/python3.4/threading.py", line 1294, in _shutdown
    t.join()
  File "/usr/local/lib/python3.4/threading.py", line 1060, in join
    self._wait_for_tstate_lock()
  File "/usr/local/lib/python3.4/threading.py", line 1076, in _wait_for_tstate_lock
    elif lock.acquire(block, timeout):
KeyboardInterrupt
$ dimint_overlord --config_path=dimint_overlord/dimint_overlord.config
ip : 115.71.237.6
OverlordStateTask works
^CException ignored in: <module 'threading' from '/usr/local/lib/python3.4/threading.py'>
Traceback (most recent call last):
  File "/usr/local/lib/python3.4/threading.py", line 1294, in _shutdown
    t.join()
  File "/usr/local/lib/python3.4/threading.py", line 1060, in join
    self._wait_for_tstate_lock()
  File "/usr/local/lib/python3.4/threading.py", line 1076, in _wait_for_tstate_lock
    elif lock.acquire(block, timeout):
KeyboardInterrupt
$ dimint_overlord --port_for_client=5556 --port_for_node=5557 --zookeeper_hosts=127.0.0.1:2121 --hash_range=10000 --max_slave_count=2
ip : 115.71.237.6
OverlordStateTask works
^CException ignored in: <module 'threading' from '/usr/local/lib/python3.4/threading.py'>
Traceback (most recent call last):
  File "/usr/local/lib/python3.4/threading.py", line 1294, in _shutdown
    t.join()
  File "/usr/local/lib/python3.4/threading.py", line 1060, in join
    self._wait_for_tstate_lock()
  File "/usr/local/lib/python3.4/threading.py", line 1076, in _wait_for_tstate_lock
    elif lock.acquire(block, timeout):
KeyboardInterrupt
```
##Configuration
* port\_for\_client: port for receive client's request and send to response to client. default is 5556.
* port\_for\_node: port for send client's request to node and receive node's request. default is 5557,
* zookeeper_hosts: zookeeper host list. If zookeeper is multiple, each host is separated by comma. default is "127.0.0.1:2181".
* hash\_range: key hash range. all keys are hashed and stored to proper nodes. default is 10000.
* max\_slave\_count: maximum slave count for each master node. default is 2.
## Etc.
* dimint 또는 dimint_overlord 프로그램이 오동작하는 경우가 있습니다. ps와 kill 명령어를 통해 강제종료시켜주시면 됩니다.
* ZooKeeper에 쓰레기 값이 남아있어 오동작하는 경우가 있습니다. python test.py 명령어로 현재 ZooKeeper 상태를 확인할 수 있습니다.
* 또, python test.py clear 명령어로 현재 ZooKeeper 에 있는 쓰레기 값을 지울 수 있습니다.
```
$ python test.py
overlord list : ['115.71.237.6:5556']
node list : ['ee4db710c82b312e0eee62ac9da024db']
master node : ee4db710c82b312e0eee62ac9da024db
$ python test.py clear
$ python test.py
overlord list : []
node list : []
```
