#!/bin/bash
# Start Vertex AI batch processing in tmux session

SESSION_NAME="vertex_batch"
PROJECT_DIR="/data/data/com.termux/files/home/projects/barcode"

# Check if tmux session already exists
if tmux has-session -t $SESSION_NAME 2>/dev/null; then
    echo "Tmux session '$SESSION_NAME' already exists."
    echo "Attaching to existing session..."
    tmux attach-session -t $SESSION_NAME
    exit 0
fi

# Create new tmux session
cd $PROJECT_DIR

tmux new-session -d -s $SESSION_NAME -n "batch_processor"

# Window 1: Batch Processor
tmux send-keys -t $SESSION_NAME:0 "cd $PROJECT_DIR && echo 'Starting Vertex AI Batch Processing...' && python3 vertex_batch_processor.py" C-m

# Window 2: Monitor
tmux new-window -t $SESSION_NAME:1 -n "monitor"
tmux send-keys -t $SESSION_NAME:1 "cd $PROJECT_DIR && echo 'Starting monitor...' && python3 monitor_batch.py" C-m

# Window 3: Logs
tmux new-window -t $SESSION_NAME:2 -n "logs"
tmux send-keys -t $SESSION_NAME:2 "cd $PROJECT_DIR && echo 'Monitoring logs...' && tail -f vertex_batch.log 2>/dev/null || echo 'No log file yet'" C-m

# Select first window and attach
tmux select-window -t $SESSION_NAME:0
echo "Tmux session '$SESSION_NAME' created with 3 windows:"
echo "  0: batch_processor - Main batch processing"
echo "  1: monitor - Real-time progress monitoring"
echo "  2: logs - Log file monitoring"
echo ""
echo "Attaching to session..."
tmux attach-session -t $SESSION_NAME