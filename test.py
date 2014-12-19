from kazoo.client import KazooClient
import sys

if __name__ == "__main__":
    kz = KazooClient('127.0.0.1:2181')
    kz.start()
    if len(sys.argv) >= 2 and sys.argv[1] == "clear":
        kz.ensure_path('/dimint/overlord/host_list')
        kz.delete('/dimint/overlord/host_list', recursive=True)
        kz.ensure_path('/dimint/overlord/host_list')
        kz.ensure_path('/dimint/node/list')
        kz.delete('/dimint/node/list', recursive=True)
        kz.ensure_path('/dimint/node/list')
        kz.ensure_path('/dimint/node/role')
        kz.delete('/dimint/node/role', recursive=True)
        kz.ensure_path('/dimint/node/role')
    else:
        kz.ensure_path('/dimint/overlord/host_list')
        print('overlord list : {0}'.format(kz.get_children('/dimint/overlord/host_list')))
        kz.ensure_path('/dimint/node/list')
        print('node list : {0}'.format(kz.get_children('/dimint/node/list')))
        kz.ensure_path('/dimint/node/role')
        for master in kz.get_children('/dimint/node/role'):
            print('master node : {0}'.format(master))
            for slave in kz.get_children('/dimint/node/role/{0}'.format(master)):
                print("\tslave node : {0}".format(slave))
    kz.stop()
