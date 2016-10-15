import logging
import sys

def generate(initial,final,config="server2.config"):
    logging.info("opening file...")
    target = open(config,'w')
    for x in range(initial,final+1):
        target.write(str(x))
        target.write("\n")

    target.close()
    return


logging.basicConfig(format='[%(levelname)s]: %(message)s',level=logging.INFO)
arg1 = int(sys.argv[1])
arg2 = int(sys.argv[2])
generate(arg1,arg2)
