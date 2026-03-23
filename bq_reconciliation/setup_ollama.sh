#!/bin/bash
# Setup script for Ollama (local LLM)

echo "================================================================================"
echo "OLLAMA SETUP - Local LLM for Testing Reports"
echo "================================================================================"

# Check if Ollama is installed
if command -v ollama &> /dev/null; then
    echo "✓ Ollama is already installed"
    ollama --version
else
    echo "Installing Ollama..."

    # Detect OS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "Detected macOS - downloading Ollama installer..."
        curl -fsSL https://ollama.com/download/Ollama-darwin.zip -o /tmp/Ollama.zip
        unzip -q /tmp/Ollama.zip -d /Applications/
        echo "✓ Ollama installed to /Applications/Ollama.app"
        echo "Please launch Ollama.app manually to start the service"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "Detected Linux - installing Ollama..."
        curl -fsSL https://ollama.com/install.sh | sh
    else
        echo "Unsupported OS. Please install Ollama manually from https://ollama.com"
        exit 1
    fi
fi

echo ""
echo "================================================================================"
echo "Starting Ollama service..."
echo "================================================================================"

# Check if Ollama service is running
if curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "✓ Ollama service is already running"
else
    echo "Starting Ollama service..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open -a Ollama
        sleep 3
    else
        ollama serve &
        sleep 3
    fi
fi

echo ""
echo "================================================================================"
echo "Downloading recommended model: llama3.2:3b (small, fast, good quality)"
echo "================================================================================"
echo "Model size: ~2GB"
echo "Download time: ~5-10 minutes depending on your connection"
echo ""

# Pull the model
ollama pull llama3.2:3b

echo ""
echo "================================================================================"
echo "SETUP COMPLETE!"
echo "================================================================================"
echo ""
echo "Available commands:"
echo "  ollama list                  - List installed models"
echo "  ollama pull <model>          - Download a model"
echo "  ollama run llama3.2:3b       - Test the model interactively"
echo ""
echo "Other recommended models for testing reports:"
echo "  ollama pull llama3.2:1b      - Smallest (1GB) - faster but less capable"
echo "  ollama pull mistral:7b       - Larger (4GB) - better quality"
echo "  ollama pull llama3.1:8b      - Largest (5GB) - highest quality"
echo ""
echo "To generate reports, run:"
echo "  python validate_from_source.py --use-bigquery --validate-derived --llm-report"
echo ""
