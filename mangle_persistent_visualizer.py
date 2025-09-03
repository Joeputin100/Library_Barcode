#!/usr/bin/env python3
"""
Mangle Persistent Enrichment Monitor
Monitors the Mangle-based enrichment architecture in real-time
"""
import time
import json
import os
import shutil
from datetime import datetime

def get_terminal_dimensions():
    """Get current terminal dimensions (columns, rows)"""
    try:
        columns, rows = shutil.get_terminal_size()
        return columns, rows
    except:
        return 80, 24

def create_progress_bar(percentage, width=40):
    """Create a visual progress bar"""
    filled = int(width * percentage / 100)
    bar = '█' * filled + '░' * (width - filled)
    return f"{bar} {percentage:.1f}%"

def analyze_enrichment_sources():
    """Analyze enrichment source usage from cumulative state file"""
    try:
        # Try to load cumulative state first
        with open("cumulative_enrichment_state.json", "r") as f:
            cumulative_state = json.load(f)
        return cumulative_state.get("source_counts_cumulative", {}), cumulative_state.get("total_records_processed", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        # Fallback to current run state
        try:
            with open("mangle_enrichment_state.json", "r") as f:
                state = json.load(f)
            return state.get("source_counts", {}), state.get("total_records", 0)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}, 0

def display_mangle_dashboard(source_counts, total_records, previous_output_lines=None):
    """Display the Mangle enrichment dashboard with ANSI rewrite to reduce flickering"""
    columns, rows = get_terminal_dimensions()
    separator_width = min(columns, 120)
    progress_bar_width = max(20, min(columns - 50, 60))
    
    output_lines = []
    
    output_lines.append("\n" + "=" * separator_width)
    output_lines.append("🔍 MANGLE ENRICHMENT MONITOR".center(separator_width))
    output_lines.append("=" * separator_width)

    # Overall Progress
    TARGET_RECORDS = 809
    overall_progress = (total_records / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
    output_lines.append(f"📊 Overall Progress: {create_progress_bar(overall_progress, progress_bar_width)}")
    output_lines.append(f"📦 Total Records Enriched: {total_records} / {TARGET_RECORDS}")
    output_lines.append("-" * separator_width)

    # Source Usage with proper percentages
    output_lines.append("🔧 ENRICHMENT SOURCE USAGE:".center(separator_width))
    output_lines.append("-" * separator_width)
    if source_counts:
        TARGET_RECORDS = 809
        
        # Display source counts with percentage of total target
        sources_to_display = ["LIBRARY_OF_CONGRESS", "GOOGLE_BOOKS", "VERTEX_AI", "OPEN_LIBRARY"]
        
        for source in sources_to_display:
            count = source_counts.get(source, 0)
            percentage = (count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
            
            # Format display names
            display_names = {
                "LIBRARY_OF_CONGRESS": "Library Of Congress",
                "GOOGLE_BOOKS": "Google Books",
                "VERTEX_AI": "Vertex Ai",
                "OPEN_LIBRARY": "Open Library"
            }
            # display_name = display_names.get(source, source.replace("_", " ").title())
            # output_lines.append(f"   {display_name:20}: {count:5} records ({percentage:.1f}%)")
        
        # Display No Enrichment separately (now integrated into bar chart)
        # no_enrich_count = source_counts.get("NO_ENRICHMENT", 0)
        # no_enrich_percentage = (no_enrich_count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
        # output_lines.append(f"   {'No Enrichment':20}: {no_enrich_count:5} records ({no_enrich_percentage:.1f}%)")
        
        # Add vertical bar graph
        output_lines.append("\n   📊 SOURCE UTILIZATION:")
        max_bar_height = 8  # Maximum height of bars in lines
        
        # Calculate bar heights and get appropriate block characters
        bar_heights = {}
        bar_blocks = {}
        for source in sources_to_display:
            count = source_counts.get(source, 0)
            percentage = (count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
            height = int((count / TARGET_RECORDS) * max_bar_height) if TARGET_RECORDS > 0 else 0
            # Ensure at least 1 block is shown for any non-zero count
            if count > 0 and height == 0:
                height = 1
            bar_heights[source] = min(height, max_bar_height)
            
            # Determine appropriate block character based on percentage
            if percentage == 0:
                bar_blocks[source] = " "  # Empty space for 0%
            elif percentage <= 50.0:
                bar_blocks[source] = "░"  # Light shade for 0.1%-50.0%
            elif percentage < 100.0:
                bar_blocks[source] = "▒"  # Medium shade for 50.1%-99.9%
            else:
                bar_blocks[source] = "▓"  # Full block for 100%
        
        # Display names for sources (3-character abbreviations for alignment)
        display_names = {
            "LIBRARY_OF_CONGRESS": "LOC",
            "GOOGLE_BOOKS": "GBK",
            "VERTEX_AI": "VAI",
            "OPEN_LIBRARY": "OLB"
        }
        
        # Print vertical bars from top to bottom
        for level in range(max_bar_height, 0, -1):
            line = "   "
            for source in sources_to_display:
                short_name = display_names.get(source, source[:6])
                if bar_heights[source] >= level:
                    line += f" {bar_blocks[source]} "  # Use appropriate block character
                else:
                    line += "   "  # Empty space
            output_lines.append(line)
        
        # Print source labels (aligned with bars - 3 characters per source)
        labels_line = "   "
        for source in sources_to_display:
            short_name = display_names.get(source, source[:3])  # Limit to 3 chars for alignment
            labels_line += f" {short_name:3} "
        output_lines.append(labels_line)
        
        # Print count/percentage information on separate line (3 chars per source)
        counts_line = "   "
        for source in sources_to_display:
            count = source_counts.get(source, 0)
            percentage = (count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
            counts_line += f" {count:2}/{percentage:2.0f}%"
        output_lines.append(counts_line)
        
        output_lines.append(f"   {'Total:':6} {sum(source_counts.get(source, 0) for source in sources_to_display):3}/{TARGET_RECORDS}")
            
    else:
        output_lines.append("   No source data available yet.")
    
    output_lines.append("-" * separator_width)

    # Record Type Distribution
    output_lines.append("📦 TARGET RECORD DISTRIBUTION:".center(separator_width))
    output_lines.append("-" * separator_width)
    try:
        with open("enriched_data_combined_mangle.json", "r") as f:
            data = json.load(f)
        
        record_types = {"MARC_B_BARCODES": 0, "ISBN_ENTRIES": 0, "NOISBN_ENTRIES": 0}
        for record in data:
            barcode = record.get("barcode", "")
            if barcode.startswith("ISBN_"):
                record_types["ISBN_ENTRIES"] += 1
            elif barcode.startswith("NOISBN_"):
                record_types["NOISBN_ENTRIES"] += 1
            elif barcode.startswith("B"):
                record_types["MARC_B_BARCODES"] += 1
        
        for rtype, count in record_types.items():
            percentage = (count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
            display_name = rtype.replace("_", " ").title()
            output_lines.append(f"   {display_name:20}: {count:4} records ({percentage:.1f}% of target)")
            
    except Exception as e:
        output_lines.append(f"   Error loading record distribution: {e}")

    output_lines.append("-" * separator_width)
    
    # Status and Timestamp
    status = "✅ COMPLETE" if overall_progress >= 100 else "🔄 PROCESSING"
    output_lines.append(f"💡 STATUS: {status}")
    output_lines.append(f"🕒 Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output_lines.append("=" * separator_width)
    output_lines.append("Press Ctrl+C to exit | Updates every 5 seconds".center(separator_width))
    
    # Use ANSI rewrite to only update changed parts
    if previous_output_lines:
        # Clear previous output by moving cursor up and clearing lines
        clear_lines = len(previous_output_lines)
        ansi_clear = f"\033[{clear_lines}A\033[0J"
        print(ansi_clear, end="")
    
    # Print current output
    for line in output_lines:
        print(line)
    
    return output_lines

def save_state(source_counts, total_records):
    """Save current state to JSON file"""
    # Calculate NO_ENRICHMENT based on actual enriched records
    total_enriched = sum(source_counts.get(source, 0) for source in ["LIBRARY_OF_CONGRESS", "GOOGLE_BOOKS", "VERTEX_AI", "OPEN_LIBRARY"])
    
    state = {
        "timestamp": datetime.now().isoformat(),
        "total_records": total_records,
        "source_counts": source_counts,
        "overall_progress": (total_records / 809) * 100 if total_records > 0 else 0
    }
    state["source_counts"]["NO_ENRICHMENT"] = 809 - total_enriched
    
    with open("mangle_enrichment_state.json", "w") as f:
        json.dump(state, f, indent=2)

def main():
    """Main function for persistent monitoring"""
    print("🚀 Starting Mangle Persistent Enrichment Monitor...")
    print("📡 Monitoring Mangle-based enrichment architecture")
    print("👀 Displaying cumulative enrichment progress")
    print("Press Ctrl+C to exit\n")
    
    # Clear screen initially
    os.system('cls' if os.name == 'nt' else 'clear')
    
    previous_output_lines = None
    
    try:
        while True:
            source_counts, total_records = analyze_enrichment_sources()
            previous_output_lines = display_mangle_dashboard(source_counts, total_records, previous_output_lines)
            # save_state(source_counts, total_records)  # Disabled to prevent state corruption
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoring stopped by user")
        print("📊 Final state saved to mangle_enrichment_state.json")
    except Exception as e:
        print(f"\n❌ Error in monitoring: {e}")

if __name__ == "__main__":
    main()
