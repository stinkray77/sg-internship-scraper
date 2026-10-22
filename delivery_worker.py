"""Poll committed deliveries while scrapers continue on their own connections."""
import threading

DELIVERY_LOCK = 734819220


class DeliveryWorker:
    def __init__(self, deliver, interval=5):
        self.deliver=deliver
        self.interval=interval
        self.stop_event=threading.Event()
        self.failures=0
        self.errors=[]
        self.thread=threading.Thread(target=self.run,name='telegram-delivery',daemon=True)

    def start(self):
        self.thread.start()

    def run(self):
        while not self.stop_event.is_set():
            try:self.failures+=self.deliver()
            except Exception as error:self.errors.append(str(error))
            self.stop_event.wait(self.interval)

    def finish(self):
        self.stop_event.set()
        self.thread.join(timeout=90)
        if self.thread.is_alive():
            self.errors.append('Delivery worker did not finish within shutdown budget; queue remains persisted')
            return
        try:self.failures+=self.deliver()
        except Exception as error:self.errors.append(str(error))
