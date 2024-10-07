from Worker import Worker

nsqd_address = '127.0.0.1:4150'
lookupd_address = '127.0.0.1:4161'
worker = Worker(nsqd_address, lookupd_address)