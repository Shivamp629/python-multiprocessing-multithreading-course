#!/usr/bin/env python3
"""
Network Packet Journey Simulation

This script simulates and visualizes how a network packet travels from the NIC
through the kernel to the application, showing the various stages and costs involved.
"""

import time
import socket
import struct
import threading
from datetime import datetime
from collections import namedtuple
from queue import Queue
import random

# Data structures to represent packet journey stages
PacketStage = namedtuple('PacketStage', ['name', 'location', 'duration_us', 'description'])
NetworkPacket = namedtuple('NetworkPacket', ['data', 'timestamp', 'src', 'dst'])

class PacketJourneySimulator:
    """Simulates the journey of a network packet through the system."""
    
    def __init__(self):
        self.stages = [
            PacketStage('NIC_RECEIVE', 'Hardware', 1, 'Packet arrives at Network Interface Card'),
            PacketStage('DMA_TRANSFER', 'Hardware', 2, 'DMA copies packet to main memory'),
            PacketStage('IRQ_SIGNAL', 'Hardware', 0.5, 'NIC raises interrupt to notify CPU'),
            PacketStage('IRQ_HANDLER', 'Kernel', 3, 'CPU executes interrupt handler'),
            PacketStage('DRIVER_PROCESS', 'Kernel', 5, 'Network driver processes packet'),
            PacketStage('NETSTACK_ENTRY', 'Kernel', 2, 'Packet enters network stack'),
            PacketStage('IP_LAYER', 'Kernel', 4, 'IP layer processing (routing, fragmentation)'),
            PacketStage('TCP_LAYER', 'Kernel', 8, 'TCP processing (sequence, ACK, window)'),
            PacketStage('SOCKET_QUEUE', 'Kernel', 2, 'Packet queued in socket buffer'),
            PacketStage('SYSCALL_WAIT', 'Kernel', 1, 'Application blocked on recv() syscall'),
            PacketStage('CONTEXT_SWITCH', 'Kernel', 15, 'Context switch to application'),
            PacketStage('DATA_COPY', 'Kernel/User', 5, 'Copy data from kernel to user buffer'),
            PacketStage('APP_PROCESS', 'User', 10, 'Application processes the data'),
        ]
        self.packet_history = []
        
    def simulate_packet_journey(self, packet_data):
        """Simulate a packet traveling through the system."""
        journey = []
        current_time = 0
        
        print(f"\n{'='*70}")
        print(f"Simulating packet journey: {len(packet_data)} bytes")
        print(f"{'='*70}\n")
        
        for stage in self.stages:
            # Simulate some variability in timing
            actual_duration = stage.duration_us * random.uniform(0.8, 1.2)
            
            journey.append({
                'stage': stage.name,
                'start_time': current_time,
                'duration': actual_duration,
                'location': stage.location,
                'description': stage.description
            })
            
            # Print stage information
            print(f"[{current_time:>6.1f} μs] {stage.location:<12} | {stage.name:<18} | {stage.description}")
            
            # Simulate processing time
            time.sleep(actual_duration / 1_000_000)  # Convert to seconds
            current_time += actual_duration
        
        total_time = current_time
        print(f"\n{'='*70}")
        print(f"Total journey time: {total_time:.1f} microseconds")
        print(f"{'='*70}")
        
        self.packet_history.append({
            'timestamp': datetime.now(),
            'data_size': len(packet_data),
            'total_time': total_time,
            'journey': journey
        })
        
        return journey
    
    def show_timing_breakdown(self, journey):
        """Display timing breakdown by system layer."""
        layer_times = {
            'Hardware': 0,
            'Kernel': 0,
            'Kernel/User': 0,
            'User': 0
        }
        
        for step in journey:
            layer_times[step['location']] += step['duration']
        
        total = sum(layer_times.values())
        
        print("\nTiming Breakdown by Layer:")
        print("-" * 40)
        for layer, time_us in sorted(layer_times.items(), key=lambda x: x[1], reverse=True):
            percentage = (time_us / total) * 100
            bar = '█' * int(percentage / 2)
            print(f"{layer:<12} | {time_us:>6.1f} μs | {percentage:>5.1f}% | {bar}")

class NetworkSimulator:
    """Simulates a simple client-server interaction to demonstrate packet flow."""
    
    def __init__(self):
        self.server_socket = None
        self.server_thread = None
        self.packet_queue = Queue()
        self.journey_simulator = PacketJourneySimulator()
        
    def start_server(self, port=0):
        """Start a simple echo server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('127.0.0.1', port))
        self.server_socket.listen(1)
        
        addr, port = self.server_socket.getsockname()
        print(f"Server listening on {addr}:{port}")
        
        self.server_thread = threading.Thread(target=self._server_loop)
        self.server_thread.daemon = True
        self.server_thread.start()
        
        return addr, port
    
    def _server_loop(self):
        """Server loop that accepts connections and echoes data."""
        while True:
            try:
                conn, addr = self.server_socket.accept()
                print(f"Server: Connection from {addr}")
                
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break
                    
                    # Simulate packet reception
                    packet = NetworkPacket(
                        data=data,
                        timestamp=datetime.now(),
                        src=addr,
                        dst=conn.getsockname()
                    )
                    self.packet_queue.put(packet)
                    
                    # Echo back
                    conn.send(data)
                
                conn.close()
            except:
                break
    
    def demonstrate_packet_flow(self):
        """Demonstrate packet flow with actual network communication."""
        # Start server
        addr, port = self.start_server()
        time.sleep(0.1)  # Let server start
        
        # Create client
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f"\nClient connecting to {addr}:{port}")
        client.connect((addr, port))
        
        # Send some data
        test_messages = [
            b"Small packet",
            b"Medium sized packet with more data" * 10,
            b"Large packet" * 100
        ]
        
        for msg in test_messages:
            print(f"\nClient sending {len(msg)} bytes...")
            client.send(msg)
            
            # Receive echo
            response = client.recv(len(msg))
            
            # Simulate the packet journey
            if not self.packet_queue.empty():
                packet = self.packet_queue.get()
                journey = self.journey_simulator.simulate_packet_journey(packet.data)
                self.journey_simulator.show_timing_breakdown(journey)
            
            time.sleep(0.5)
        
        # Cleanup
        client.close()
        self.server_socket.close()

def show_kernel_user_boundary():
    """Visualize the kernel/user space boundary and copy costs."""
    print("\n" + "="*70)
    print("Kernel/User Space Boundary")
    print("="*70)
    
    print("""
    User Space                    |  Kernel Space
    ----------------------------- | -----------------------------
                                  |
    Application Buffer  <---------|-------- Socket Buffer
         (malloc)         copy    |         (kernel memory)
                                  |              ^
                                  |              |
    recv() system call  --------->|         TCP/IP Stack
         (blocks)                 |              ^
                                  |              |
                                  |         Network Driver
                                  |              ^
                                  |              |
                                  |            NIC DMA
                                  |              ^
                                  |              |
    ============================= | =============================
                  Hardware        |        Network Packet
    """)
    
    print("\nCost of crossing the boundary:")
    print("- Context switch: ~1-3 microseconds")
    print("- Data copy: ~0.5 microseconds per KB")
    print("- System call overhead: ~0.1-0.3 microseconds")
    print("\nOptimizations to reduce boundary crossings:")
    print("- Batch operations (recvmmsg, sendmmsg)")
    print("- Memory mapping (mmap)")
    print("- Zero-copy techniques (splice, sendfile)")
    print("- Kernel bypass (DPDK, io_uring)")

def measure_real_latencies():
    """Measure real system latencies for comparison."""
    print("\n" + "="*70)
    print("Real System Latency Measurements")
    print("="*70)
    
    iterations = 1000
    
    # Measure loopback latency
    print("\nMeasuring loopback network latency...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', 0))
    server.listen(1)
    addr, port = server.getsockname()
    
    # Start echo server
    def echo_server():
        conn, _ = server.accept()
        while True:
            data = conn.recv(1024)
            if not data:
                break
            conn.send(data)
        conn.close()
    
    server_thread = threading.Thread(target=echo_server)
    server_thread.daemon = True
    server_thread.start()
    
    # Connect client
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((addr, port))
    
    # Measure round-trip time
    msg = b"ping"
    latencies = []
    
    for _ in range(iterations):
        start = time.perf_counter()
        client.send(msg)
        response = client.recv(len(msg))
        end = time.perf_counter()
        latencies.append((end - start) * 1_000_000)  # Convert to microseconds
    
    client.close()
    server.close()
    
    # Calculate statistics
    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)
    
    print(f"\nLoopback TCP Round-Trip Latency:")
    print(f"  Average: {avg_latency:.1f} μs")
    print(f"  Minimum: {min_latency:.1f} μs")
    print(f"  Maximum: {max_latency:.1f} μs")
    
    # Measure system call overhead
    print("\nMeasuring system call overhead...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    start = time.perf_counter()
    for _ in range(iterations):
        sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
    end = time.perf_counter()
    
    syscall_time = (end - start) / iterations * 1_000_000
    print(f"  getsockopt() syscall: {syscall_time:.2f} μs per call")
    
    sock.close()

def main():
    """Run the packet journey demonstration."""
    print("Network Packet Journey Demonstration")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Show kernel/user boundary
        show_kernel_user_boundary()
        
        # Simulate packet journey
        simulator = NetworkSimulator()
        simulator.demonstrate_packet_flow()
        
        # Measure real latencies
        measure_real_latencies()
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*70)
    print("Demonstration complete!")

if __name__ == "__main__":
    main()
