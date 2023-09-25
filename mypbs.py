import redis
import argparse
import json
import time
from pprint import pprint
from traceback import print_exc
import subprocess


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="guest")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=6379)
    return parser.parse_args()

def parse_queue_jsons(q):
    return [json.loads(s) for s in q]

class MyPBS:

    def __init__(self, name: str, host: str, port: int = 6379) -> None:
        self.redis = redis.StrictRedis(
            host=host,
            port=port,
            db=0,
            decode_responses=True
            )
        self.name = name

    def join(self):
        self.redis.set(f"node.{self.name}", "waiting")
        print(f"node {self.name} start")


    def leave(self):
        self.redis.delete(f"node.{self.name}")


    def start_consuming(self):
        print("start consuming...")
        while True:
            command_json = self.redis.lpop("cmd.waiting", 1)
            if command_json is None:
                time.sleep(10)
                continue
            else:
                command_json = command_json[0]
            
            print(command_json)
            command = json.loads(str(command_json))
            try:
                print("Load job")
                pprint(command)

                self.redis.set(f"node.{self.name}", "running " + command["name"])

                b = subprocess.check_output(command["command"], shell=True)
                print("Result for", command["name"])

                self.redis.set(f"node.{self.name}", "waiting")

                command["result"] = "success"
            except:
                print_exc()
                command["result"] = "error"
            
            command = self.redis.lpush("cmd.finish", json.dumps(command))

    def get_nodes(self):
        return {key[5:]: self.redis.get(key) for key in self.redis.scan_iter("node.*")}
            
    
    def get_waiting_jobs(self):
        return parse_queue_jsons(self.redis.lrange(f"cmd.waiting", 0, -1))
    
    def get_finished_jobs(self):
        return parse_queue_jsons(self.redis.lrange(f"cmd.finish", 0, -1))
    
    def add_job(self, name, command):
        job = dict(
            name=name,
            command=command
        )
        self.redis.rpush("cmd.waiting", json.dumps(job))

    def delete(self):
        for key in self.redis.keys('*'):
            self.redis.delete(key)