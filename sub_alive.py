
# Example usage for MessageProcessor:
from datetime import datetime
import nsq
from Worker import MessageProcessor

lookupd_http_address = '127.0.0.1:4161'

alive_dict = {}
def check_last_alive(msg):
    alive_dict[msg.body.decode()] = f'{datetime.now()}'
    print(alive_dict)
    
worker_alive = MessageProcessor('alive', 'general', lookupd_http_address, check_last_alive)
nsq.run()