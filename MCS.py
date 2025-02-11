"""
Base module for dealing with MCS communication.  This module provides the
MCSComminunicate framework that specified how to processes MCS commands that
arrive via UDP.
"""

from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any, Union
import queue
import socket
import threading
import time
from datetime import datetime
import uuid

@dataclass
class MCSMessage:
    """Core message class for MCS protocol"""
    destination: str 
    sender: str
    command: str
    reference: int
    data: Optional[Union[str, bytes]] = None
    dst_ip: Optional[str] = None
    src_ip: Optional[str] = None
    
    @property
    def data_length(self) -> int:
        """Get length of data payload"""
        return len(self.data) if self.data else 0
    
    def encode(self) -> bytes:
        """Encode message for network transmission"""
        mjd, mpm = get_time()
        header = (
            f"{self.destination:<3}"
            f"{self.sender:<3}"
            f"{self.command:<3}"
            f"{self.reference:09d}"
            f"{self.data_length:04d}"
            f"{mjd:06d}"
            f"{mpm:09d} "
        ).encode()
        
        if isinstance(self.data, str):
            data = self.data.encode()
        else:
            data = self.data if self.data else b''
            
        return header + data
    
    @classmethod
    def decode(cls, packet: bytes, src_ip: Optional[str] = None) -> 'MCSMessage':
        """Decode network packet into message"""
        header = packet[:38].decode()
        data = packet[38:38 + int(header[18:22])]
        
        return cls(
            destination=header[:3].strip(),
            sender=header[3:6].strip(), 
            command=header[6:9].strip(),
            reference=int(header[9:18]),
            data=data,
            src_ip=src_ip
        )
    
    def create_reply(self, accept: bool, status: str, data: Union[str, bytes] = b'') -> 'MCSMessage':
        """Create a reply message"""
        response = 'A' if accept else 'R'
        reply_data = response.encode() + f"{status:>7}".encode()
        
        if isinstance(data, str):
            reply_data += data.encode()
        else:
            reply_data += data
            
        return MCSMessage(
            destination=self.sender,
            sender=self.destination,
            command=self.command,
            reference=self.reference,
            data=reply_data,
            dst_ip=self.src_ip
        )

class MessageReceiver(threading.Thread):
    """Thread for receiving MCS messages"""
    
    def __init__(self, address: Tuple[str, int], subsystem: str = 'ALL'):
        super().__init__()
        self.subsystem = subsystem
        self.address = address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        self.socket.bind(address)
        self.msg_queue = queue.Queue()
        self.running = threading.Event()
        self.daemon = True
    
    def run(self):
        self.running.set()
        while self.running.is_set():
            try:
                data, addr = self.socket.recvfrom(16384)
                if data:
                    msg = MCSMessage.decode(data, addr[0])
                    if (self.subsystem == 'ALL' or
                        msg.destination == 'ALL' or
                        msg.destination == self.subsystem):
                        self.msg_queue.put(msg)
            except socket.error:
                pass
                
    def stop(self):
        self.running.clear()
        self.socket.close()
        
    def get_message(self, timeout: Optional[float] = None) -> Optional[MCSMessage]:
        try:
            return self.msg_queue.get(timeout=timeout)
        except queue.Empty:
            return None

class MessageSender(threading.Thread):
    """Thread for sending MCS messages"""
    
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
        while self.running.set():
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
                        time.sleep(0.001)
                        
            except queue.Empty:
                continue
                
    def stop(self):
        self.running.clear()
        self.socket.close()
        
    def send_message(self, msg: MCSMessage):
        self.msg_queue.put(msg)

class MCSClient:
    """High-level client for MCS communication"""
    
    def __init__(self, address: Tuple[str, int], subsystem: str = 'MCS'):
        self.subsystem = subsystem
        self.sender = MessageSender(address, subsystem)
        recv_addr = ('0.0.0.0', address[1] + 1)
        self.receiver = MessageReceiver(recv_addr, subsystem)
        
    def send_command(self, 
                     destination: str,
                     command: str, 
                     data: Union[str, bytes] = b'',
                     timeout: float = 5.0) -> MCSMessage:
        """Send command and wait for response"""
        msg = MCSMessage(
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
            
        if response.data[:1] != b'A':
            raise RuntimeError(f"Command rejected: {response.data[1:].decode()}")
            
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
def get_reference() -> int:
    ref = 0
    while ref == 0 or ref == 999999999:
        ref = int.from_bytes(uuid.uuid1().bytes[:4], byteorder='big')
        ref %= 1000000000
    return ref

def get_time() -> Tuple[int, int]:
    """Get current MJD and MPM"""
    dt = datetime.utcnow()
    
    # MJD calculation
    a = (14 - dt.month) // 12
    y = dt.year + 4800 - a
    m = dt.month + (12 * a) - 3
    
    mjd = (dt.day + ((153 * m + 2) // 5) + 365 * y + y // 4 
           - y // 100 + y // 400 - 32045)
    
    # MPM calculation  
    mpm = ((dt.hour * 3600 + dt.minute * 60 + dt.second) * 1000 
           + dt.microsecond // 1000)
           
    return mjd, mpm
