import sys
import dimint_overlord.Overlord
import os
import psutil

def print_dimint_help():
    print('''
    dimint help
    dimint overlord help
    dimint overlord list
    dimint overlord start
    dimint overlord stop
    ''')

def print_dimint_overlord_help():
    print('''
    dimint overlord help
    dimint overlord list
    dimint overlord start
    dimint overlord stop
    ''')

def main():
    if len(sys.argv) == 1:
        print_dimint_help()
    elif sys.argv[1] == 'help':
        print_dimint_help()
    elif sys.argv[1] == 'overlord':
        if len(sys.argv) == 2:
            print_dimint_overlord_help()
        elif sys.argv[2] == 'help':
            print_dimint_overlord_help()
        elif sys.argv[2] == 'list':
            for p in psutil.process_iter():
                if p.name() in ['python', 'dimint', 'dimint_overlord']:
                    print('{0}, cmdline : {1}'.format(p, p.cmdline()))
        elif sys.argv[2] == 'start':
            newpid = os.fork()
            if newpid == 0:
                sys.stdout = open('Overlord_{0}.log'.format(os.getpid()), 'w')
                dimint_overlord.Overlord.main(sys.argv[3:])
            else:
                print('Hello from parent', os.getpid(), newpid)
        elif sys.argv[2] == 'stop':
            if len(sys.argv) == 3:
                print('pid is needed')
            else:
                pid = int(sys.argv[3])
                p = psutil.Process(pid)
                p.send_signal(10)

if __name__ == '__main__':
    main()
