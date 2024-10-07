
from datetime import datetime
from threading import Thread
import tornado
from Worker import MessageQueuePublisher

nsqd_address = '127.0.0.1:4150'
lookupd_address = '127.0.0.1:4161'

sender = MessageQueuePublisher('task', nsqd_address)
sender.put(f'a general task {datetime.now()}')
sender.put('a df2f1d69-4845-4f4e-bf9b-b150670acef8 task')
sender.put(f'a  global task {datetime.now()}')
sender.put(sender.SIGNAL_STOP)
