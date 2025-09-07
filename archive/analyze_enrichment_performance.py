#!/usr/bin/env python3
"""
Analyze enrichment performance from test results
"""
import re
import json
from datetime import datetime

def analyze_performance(log_content):
    """Analyze performance from test output"""
    
    # Extract processing times
    time_pattern = r'Enrichment completed in (\d+\.\d+)s'
    times = [float(match) for match in re.findall(time_pattern, log_content)]
    
    # Extract success/failure counts
    success_pattern = r'✅ Enrichment completed'
    error_pattern = r'❌ Error'
    
    successes = len(re.findall(success_pattern, log_content))
    errors = len(re.findall(error_pattern, log_content))
    
    # Extract API success rates
    api_patterns = {
        'Google Books': r'Google Books: ✅',
        'Library of Congress': r'Library of Congress: ✅', 
        'Open Library': r'Open Library: ✅',
        'Vertex AI': r'Vertex AI: ✅'
    }
    
    api_success = {}
    for api, pattern in api_patterns.items():
        api_success[api] = len(re.findall(pattern, log_content))
    
    # Extract Vertex AI quality scores
    quality_pattern = r'Vertex AI Quality: (\d+)/10'
    quality_scores = [int(match) for match in re.findall(quality_pattern, log_content)]
    
    # Calculate statistics
    if times:
        total_time = sum(times)
        avg_time = total_time / len(times)
        min_time = min(times)
        max_time = max(times)
        
        # Estimate full batch
        full_batch_time = avg_time * 809
        full_batch_hours = full_batch_time / 3600
        
        print("📊 ENRICHMENT PERFORMANCE ANALYSIS")
        print("=" * 50)
        print(f"Records processed: {len(times)}/{10}")
        print(f"Success rate: {successes}/{len(times)} ({successes/len(times)*100:.1f}%)")
        print(f"Errors: {errors}")
        print()
        
        print("⏱️ PROCESSING TIMES:")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Average per record: {avg_time:.2f}s")
        print(f"  Fastest: {min_time:.2f}s")
        print(f"  Slowest: {max_time:.2f}s")
        print()
        
        print("🌐 API SUCCESS RATES:")
        for api, count in api_success.items():
            success_rate = (count / len(times)) * 100
            print(f"  {api:20}: {count:2d}/10 ({success_rate:5.1f}%)")
        print()
        
        if quality_scores:
            avg_quality = sum(quality_scores) / len(quality_scores)
            print("🧠 VERTEX AI QUALITY:")
            print(f"  Average quality score: {avg_quality:.1f}/10")
            print(f"  Range: {min(quality_scores)}-{max(quality_scores)}/10")
            print()
        
        print("📈 FULL BATCH ESTIMATE (809 records):")
        print(f"  Estimated total time: {full_batch_time:.2f}s ({full_batch_hours:.2f} hours)")
        print(f"  Estimated completion: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        # Cloud vs local comparison
        cloud_speedup = 4  # 4x faster with parallel workers
        cloud_time = full_batch_time / cloud_speedup
        cloud_hours = cloud_time / 3600
        
        print(f"\n☁️  GOOGLE CLOUD ESTIMATE (4 workers):")
        print(f"  Estimated time: {cloud_time:.2f}s ({cloud_hours:.2f} hours)")
        print(f"  Speedup: {cloud_speedup}x faster")
        print(f"  Time saved: {full_batch_hours - cloud_hours:.2f} hours")

if __name__ == "__main__":
    # Read the test output
    with open('test_output.log', 'r') as f:
        log_content = f.read()
    
    analyze_performance(log_content)