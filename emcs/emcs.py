"""
Base module for dealing with enhanced MCS communication.  This module provides
the Client framework that specified how to processes enhanced MCS commands that
arrive via UDP.
"""

from dataclasses import dataclass, asdict
from typing import Optional, Tuple, Dict, Any, Union
import queue
import socket
import threading
import time
from datetime import datetime
import uuid
import ubjson
import math

@dataclass
class Message:
    """Core message class for enhanced MCS protocol"""
    destination: str 
    sender: str
    command: str
    reference: str
    data: Optional[Union[str, bytes, Dict]] = None
    dst_ip: Optional[str] = None
    src_ip: Optional[str] = None
    timestamp: Optional[Dict] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            mjd, mpm = get_time()
            self.timestamp = {
                "mjd": mjd,
                "mpm": mpm,
                "scale": 'UTC'
            }
    
    def to_json(self) -> Dict:
        mjd, mpm = get_time()
        msg_dict = asdict(self)
        return msg_dict
        
    def encode(self) -> bytes:
        json_data = self.to_json()
        return ubjson.dumpb(json_data)
    
    @classmethod
    def decode(cls, packet: bytes, src_ip: Optional[str] = None) -> 'Message':
        data = ubjson.loadb(packet)
        return cls(**data)
        
    def create_reply(self, accept: bool, status: str, data: Union[str, bytes] = b'') -> 'Message':
        reply_data = {'status': status,
                      'accepted': accept,
                      'data': None}
        if data:
            reply_data['data'] = data
            
        return Message(
            destination=self.sender,
            sender=self.destination,
            command=self.command,
            reference=self.reference,
            data=reply_data,
            dst_ip=self.src_ip
        )

class MessageReceiver(threading.Thread):
    """Thread for receiving enhanced MCS messages"""
    
    def __init__(self, address: Tuple[str, int], subsystem: str = 'ALL'):
        super().__init__()
        self.subsystem = subsystem
        self.address = address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        self.socket.bind(address)
        self.socket.settimeout(5.0)
        self.msg_queue = queue.Queue()
        self.running = threading.Event()
        self.daemon = True
    
    def run(self):
        self.running.set()
        while self.running.is_set():
            try:
                data, addr = self.socket.recvfrom(16384)
                if data:
                    msg = Message.decode(data, addr[0])
                    if (self.subsystem == 'ALL' or
                        msg.destination == 'ALL' or
                        msg.destination == self.subsystem):
                        self.msg_queue.put(msg)
            except socket.error:
                pass
                
    def stop(self):
        self.running.clear()
        self.socket.close()
        
    def get_message(self, timeout: Optional[float] = None) -> Optional[Message]:
        try:
            return self.msg_queue.get(timeout=timeout)
        except queue.Empty:
            return None

class MessageSender(threading.Thread):
    """Thread for sending enhanced MCS messages"""
    
    def __init__(self, address: Tuple[str, int], subsystem: str):
        super().__init__()
        self.subsystem = subsystem
        self.address = address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.msg_queue = queue.Queue()
        self.running = threading.Event()
        self.daemon = True
        
    def run(self):
        self.running.set()
        while self.running.is_set():
            try:
                msg = self.msg_queue.get(timeout=1.0)
                if msg is None:
                    continue
                    
                msg.sender = self.subsystem
                encoded = msg.encode()
                dst_ip = msg.dst_ip if msg.dst_ip else self.address[0]
                dst_port = self.address[1]
                
                for _ in range(3): # Retry logic
                    try:
                        self.socket.sendto(encoded, (dst_ip, dst_port))
                        break
                    except socket.error:
                        time.sleep(0.005)
                        
            except queue.Empty:
                continue
                
    def stop(self):
        self.running.clear()
        self.socket.close()
        
    def send_message(self, msg: Message):
        self.msg_queue.put(msg)

class Server:
    """High-level server for enhanced MCS communication"""

    def __init__(self, address: Tuple[str, int], subsystem: str = 'MCS'):
        self.subsystem = subsystem
        send_addr = ('0.0.0.0', address[1] + 1)
        self.sender = MessageSender(send_addr, subsystem)
        self.receiver = MessageReceiver(address, subsystem)
        
    def receive_command(self, timeout: float = 5.0) -> Optional[Message]:
        return self.receiver.get_message(timeout=timeout)
        
    def send_reply(self, response: Message):
        self.sender.send_message(response)
        
    def start(self):
        self.sender.start()
        self.receiver.start()
        
    def close(self):
        self.sender.stop()
        self.receiver.stop()
        self.sender.join()
        self.receiver.join()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class Client:
    """High-level client for enhanced MCS communication"""
    
    def __init__(self, address: Tuple[str, int], subsystem: str = 'MCS'):
        self.subsystem = subsystem
        send_addr = ('0.0.0.0', address[1] - 1)
        self.sender = MessageSender(send_addr, subsystem)
        self.receiver = MessageReceiver(address, subsystem)
        
    def send_command(self, 
                     destination: str,
                     command: str, 
                     data: Union[str, bytes] = b'',
                     timeout: float = 5.0) -> Message:
        """Send command and wait for response"""
        msg = Message(
            destination=destination,
            sender=self.subsystem,
            command=command,
            reference=get_reference(),
            data=data
        )
        
        self.sender.send_message(msg)
        response = self.receiver.get_message(timeout)
        
        if not response:
            raise TimeoutError("No response received")
            
        if response.data['accepted'] != True:
            raise RuntimeError(f"Command rejected: {response.data}")
            
        return response
        
    def start(self):
        self.sender.start()
        self.receiver.start()
        
    def close(self):
        self.sender.stop()
        self.receiver.stop()
        self.sender.join()
        self.receiver.join()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Utility functions
def get_reference() -> str:
    return str(uuid.uuid1())

def get_time() -> Tuple[int, int]:
    """
    Return a two-element tuple of the current MJD and MPM.
    """
    
    # determine current time
    dt = datetime.utcnow()
    year        = dt.year             
    month       = dt.month      
    day         = dt.day    
    hour        = dt.hour
    minute      = dt.minute
    second      = dt.second     
    millisecond = dt.microsecond / 1000
    
    # compute MJD         
    # adapted from http://paste.lisp.org/display/73536
    # can check result using http://www.csgnetwork.com/julianmodifdateconv.html
    a = (14 - month) // 12
    y = year + 4800 - a          
    m = month + (12 * a) - 3                    
    p = day + (((153 * m) + 2) // 5) + (365 * y)   
    q = (y // 4) - (y // 100) + (y // 400) - 32045
    mjd = int(math.floor( (p+q) - 2400000.5))  
    
    # compute MPM
    mpm = int(math.floor( (hour*3600 + minute*60 + second)*1000 + millisecond ))
    
    return (mjd, mpm)
