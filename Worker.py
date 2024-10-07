import threading
import uuid
from queue import Queue
import nsq
import tornado.ioloop

class MessageProcessor:
    def __init__(self, msg_topic, msg_channel, lookupd_http_address, process_message):
        self.msg_topic = msg_topic
        self.msg_channel = msg_channel
        self._msg_queue = Queue()

        # Initialize NSQ reader and start message processing
        self._create_reader(lookupd_http_address)
        self._start_message_processing(process_message)
        # nsq.run()

    def _create_reader(self, lookupd_http_address):
        """Sets up an NSQ reader to listen for messages on the specified topic and channel."""
        try:
            nsq.Reader(
                message_handler=self._message_callback,
                topic=self.msg_topic,
                channel=self.msg_channel,
                # nsqd_tcp_addresses=[nsqd_address],
                lookupd_http_addresses=[lookupd_http_address],
                max_in_flight=10
            )
        except Exception as e:
            print(f"Error creating NSQ reader for {self.msg_topic} on {self.msg_channel}: {e}")

    def _message_callback(self, msg:nsq.Message):
        """Handles and queues incoming messages for processing."""
        try:
            # print(f"Received message on {self.msg_channel}: {msg.body}")
            self._msg_queue.put(msg)
            msg.finish()  # Acknowledge message as processed
        except Exception as e:
            print(f"Error processing message: {e}")
            msg.requeue()  # Requeue message if processing fails

    def _queue_loop(self,process_message):
        while True:
            msg = self._msg_queue.get()
            if msg is None:continue
            process_message(msg)
            # print(f"Processing general msg: {msg}")
            # Add actual task processing logic here
            self._msg_queue.task_done()

    def _start_message_processing(self, process_message):
        """Starts message processing in a separate thread."""
        self.processing_thread = threading.Thread(target=self._queue_loop, args=(process_message,), daemon=True)
        self.processing_thread.start()

    def shutdown(self):
        """Signals a graceful shutdown by putting a shutdown marker in the queue and waits for thread completion."""
        self._msg_queue.put(None)
        self.processing_thread.join()

class MessagePublisher:
    def __init__(self, obj, name, msg_topic, nsqd_address, period=1000):
        self.obj = obj
        self.name = name
        self.msg_topic = msg_topic
        self.nsqd_address = nsqd_address
        self.period = period

        self.writer = nsq.Writer([nsqd_address])
        self.pub_callback = tornado.ioloop.PeriodicCallback(self.pub_message, period)
        self.pub_callback.start()
        # nsq.run()

    def pub_message(self):
        """Publishes a message periodically."""
        try:
            self.writer.pub(self.msg_topic, str(getattr(self.obj, self.name)).encode(), self.finish_pub)
        except Exception as e:
            print(f"Error publishing message: {e}")

    def finish_pub(self, conn, data):
        """Callback after publishing."""
        print(f"Publish finished: {data}")

    def shutdown(self):
        """Stops the periodic publishing."""
        self.pub_callback.stop()

class MessageQueuePublisher(MessagePublisher):
    SIGNAL_STOP = 'stop'

    def __init__(self, msg_topic, nsqd_address, period=1000):
        super().__init__(self, 'queue', msg_topic, nsqd_address, period)
        self.queue = Queue()
        threading.Thread(target=tornado.ioloop.IOLoop.current().start).start()

    def put(self,obj):
        return self.queue.put(obj)
    
    def pub_message(self):
        """Publishes a message periodically."""
        try:
            queue:Queue = getattr(self.obj, self.name)
            if not queue.empty():
                msg = str(queue.get())
                if msg == self.SIGNAL_STOP:
                    tornado.ioloop.IOLoop.current().stop()
                self.writer.pub(self.msg_topic, msg.encode(), self.finish_pub)
                queue.task_done()
        except Exception as e:
            print(f"Error publishing message: {e}")

class Worker:
    def __init__(self, lookupd_http_address):
        self.worker_id = str(uuid.uuid4())
        self.task_topic = 'task'
        self.alive_topic = 'alive'
        self.nsqd_address = '0.0.0.0'

        # Initialize alive signal publisher
        self.alive_publisher = MessagePublisher(self, 'worker_id', self.alive_topic, nsqd_address)

        # Initialize message processors for general and global tasks
        self._general_task = MessageProcessor(
            self.task_topic, 'general', lookupd_http_address, self._process_general_task
        )
        self._global_task = MessageProcessor(
            self.task_topic, self.worker_id, lookupd_http_address, self._process_specify_task
        )

        # Start the NSQ event loop
        self._start_nsq_event_loop()

    def worker_print(self,msg):
        print(f"[Worker:{self.worker_id}]: {msg}")

    def _process_general_task(self, msg: nsq.Message):
        msg:str = msg.body.decode()
        if 'general' in msg.lower():
            self.worker_print(f"Processing general task: {msg}")
            
    def _process_specify_task(self, msg: nsq.Message):
        msg:str = msg.body.decode()
        if 'global' in msg.lower():
            self.worker_print(f"Processing task: {msg}")            
        
        if self.worker_id in msg.lower():
            self.worker_print(f"Processing task: {msg}")
            
    def _start_nsq_event_loop(self):
        """Starts the NSQ event loop."""
        try:
            nsq.run()
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        """Gracefully shuts down the worker, stopping message publishing and the Tornado event loop."""
        print("Shutting down gracefully...")
        self.alive_publisher.shutdown()
        self._general_task.shutdown()
        self._global_task.shutdown()
        tornado.ioloop.IOLoop.instance().stop()
        print("Shutdown complete.")


# Example usage for MessageProcessor:
def example_process_message(msg):
    print(f"Processing message: {msg}")

nsqd_address = '127.0.0.1:4150'
lookupd_http_address = '127.0.0.1:4161'

def ex1():
    # Setting up the message processor
    msg_topic = 'example_topic'
    msg_channel = 'example_channel'
    processor = MessageProcessor(msg_topic, msg_channel, nsqd_address, lookupd_http_address, example_process_message)

    # Example usage for MessagePublisher:
    # Publishing a message based on an object attribute
    class ExampleObject:
        def __init__(self):
            self.example_attr = "Hello, NSQ!"

    example_obj = ExampleObject()
    publisher = MessagePublisher(example_obj, 'example_attr', 'example_topic', nsqd_address)

    nsq.run()
    # To simulate graceful shutdown:
    import time

    try:
        time.sleep(100)  # Simulate running worker for 10 seconds
    finally:
        publisher.shutdown()
        processor.shutdown()

def ex2():
    # Example usage for Worker:
    # Initialize a worker that processes tasks and sends alive signals
    worker_alive = MessageProcessor('alive', 'general', nsqd_address, lookupd_http_address, example_process_message)
    worker = Worker(nsqd_address, lookupd_http_address)

    # To simulate graceful shutdown:
    import time

    try:
        time.sleep(100)  # Simulate running worker for 10 seconds
    finally:
        worker.shutdown()


# ex1()
# ex2()