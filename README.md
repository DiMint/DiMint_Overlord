DiMint_Overlord
===============
##Installation
```
$ virtualenv -p /usr/bin/python3 myenv
$ source ./myenv/bin/activate
$ python setup.py install
$ dimint_overlord
$ deactivate
```
##Execution
```
$ source ./myenv/bin/activate
$ dimint
$ dimint_overlord
$ deactivate
```
##Configuration
* port\_for\_client: port for receive client's request and send to response to client. default is 5556.
* port\_for\_node: port for send client's request to node and receive node's request. default is 5557,
* zookeeper_hosts: zookeeper host list. If zookeeper is multiple, each host is separated by comma. default is "127.0.0.1:2181".
* hash\_range: key hash range. all keys are hashed and stored to proper nodes. default is 10000.
* max\_slave\_count: maximum slave count for each master node. default is 2.

### How to use
```bash
$ dimint_node --port_for_client=5556 --port_for_node=5557 --zookeeper_hosts=127.0.0.1:2121 --hash_range=10000 --max_slave_count=2
```
or
```bash
$ dimint_node --config_path=dimint_overlord.config
```
which dimint_overlord.config contains this content:
```json
{
  "port_for_client": 5556,
  "port_for_node": 5557,
  "zookeeper_hosts": "127.0.0.1:2181",
  "hash_range": 10000,
  "max_slave_count": 2
}
```
