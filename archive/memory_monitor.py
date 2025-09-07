#!/usr/bin/env python3
"""
Memory usage monitor for enrichment engine
"""
import time
import psutil
import subprocess
from datetime import datetime

def get_memory_usage():
    """Get memory usage of Python processes"""
    try:
        # Get memory usage of all Python processes
        python_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
            try:
                if 'python' in proc.info['name'].lower():
                    mem_mb = proc.info['memory_info'].rss / 1024 / 1024
                    python_processes.append({
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'memory_mb': mem_mb
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        # Get total system memory usage
        total_memory = psutil.virtual_memory()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'python_processes': python_processes,
            'total_memory': {
                'total_mb': total_memory.total / 1024 / 1024,
                'used_mb': total_memory.used / 1024 / 1024,
                'available_mb': total_memory.available / 1024 / 1024,
                'percent_used': total_memory.percent
            }
        }
    except Exception as e:
        return {'error': str(e)}

def monitor_memory(interval=10):
    """Continuously monitor memory usage"""
    print("🧠 Starting Memory Monitor...")
    print("📊 Monitoring Python process memory usage")
    print("⏰ Update interval:", interval, "seconds")
    print("Press Ctrl+C to exit\n")
    
    try:
        while True:
            memory_info = get_memory_usage()
            
            if 'error' in memory_info:
                print(f"❌ Error: {memory_info['error']}")
            else:
                # Clear screen and display memory info
                print("\033[H\033[2J", end="")  # Clear screen
                print(f"📅 {memory_info['timestamp']}")
                print("=" * 50)
                
                # Display system memory
                total_mem = memory_info['total_memory']
                print(f"💾 System Memory: {total_mem['used_mb']:.1f}MB / {total_mem['total_mb']:.1f}MB ({total_mem['percent_used']:.1f}% used)")
                print(f"   Available: {total_mem['available_mb']:.1f}MB")
                print("-" * 50)
                
                # Display Python processes
                print("🐍 Python Processes:")
                for proc in memory_info['python_processes']:
                    print(f"   PID {proc['pid']}: {proc['name']} - {proc['memory_mb']:.1f}MB")
                
                print("-" * 50)
                print("⏰ Next update in", interval, "seconds...")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Memory monitoring stopped")

if __name__ == "__main__":
    monitor_memory(interval=10)