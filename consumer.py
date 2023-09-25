from mypbs import get_args, MyPBS
import pandas as pd

args = get_args()
pbs = MyPBS(args.name, args.host, args.port)

pbs.join()
pbs.start_consuming()
pbs.leave()