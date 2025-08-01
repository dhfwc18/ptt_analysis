.DEFAULT_GOAL := all
.PHONY: all init crawl clean

# Detect OS
ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
    VENV_ACTIVATE := .venv\Scripts\activate.bat
    VENV_PYTHON := .venv\Scripts\python.exe
    VENV_PIP := .venv\Scripts\pip.exe
    PATH_SEP := \\
    MKDIR := if not exist
    RMDIR := rmdir /s /q
    DEL := del /q
    CALL := call
    SET_PYTHONPATH := set PYTHONPATH=.
else
    DETECTED_OS := $(shell uname -s)
    VENV_ACTIVATE := .venv/bin/activate
    VENV_PYTHON := .venv/bin/python
    VENV_PIP := .venv/bin/pip
    PATH_SEP := /
    MKDIR := mkdir -p
    RMDIR := rm -rf
    DEL := rm -f
    CALL := source
    SET_PYTHONPATH := export PYTHONPATH=.
endif

all: init crawl

init:
	@echo → Setting up development environment for $(DETECTED_OS)...
ifeq ($(OS),Windows_NT)
	@if not exist .venv ( \
		echo → Creating virtualenv... && \
		python -m venv .venv \
	) else ( \
		echo → Virtual environment already exists \
	)
	@echo → Activating virtualenv and installing dependencies...
	@$(CALL) $(VENV_ACTIVATE) && \
		echo → Upgrading pip... && \
		python -m pip install --upgrade pip && \
		echo → Installing project in development mode... && \
		pip install -e .
else
	@if [ ! -d ".venv" ]; then \
		echo "→ Creating virtualenv..."; \
		python3 -m venv .venv; \
	else \
		echo "→ Virtual environment already exists"; \
	fi
	@echo "→ Activating virtualenv and installing dependencies..."
	@$(CALL) $(VENV_ACTIVATE) && \
		echo "→ Upgrading pip..." && \
		python -m pip install --upgrade pip && \
		echo "→ Installing project in development mode..." && \
		pip install -e .
endif

crawl:
	@echo → Running crawler...
ifeq ($(OS),Windows_NT)
	@if not exist .venv ( \
		echo → Virtual environment not found. Run 'make init' first. && \
		exit /b 1 \
	)
	@$(CALL) $(VENV_ACTIVATE) && \
		cd src && \
		$(SET_PYTHONPATH) && \
		python crawl_party_data.py
else
	@if [ ! -d ".venv" ]; then \
		echo "→ Virtual environment not found. Run 'make init' first."; \
		exit 1; \
	fi
	@$(CALL) $(VENV_ACTIVATE) && \
		cd src && \
		$(SET_PYTHONPATH) && \
		python pipeline/crawl_party_data.py
endif

clean:
	@echo → Cleaning up build artifacts and cache files...
ifeq ($(OS),Windows_NT)
	@echo → Removing __pycache__ directories...
	@for /d /r . %%d in (__pycache__) do @if exist "%%d" $(RMDIR) "%%d" 2>nul
	@echo → Removing .pyc files...
	@for /r . %%f in (*.pyc) do @if exist "%%f" $(DEL) "%%f" 2>nul
	@echo → Removing .pyo files...
	@for /r . %%f in (*.pyo) do @if exist "%%f" $(DEL) "%%f" 2>nul
	@echo → Removing egg-info directories...
	@for /d /r . %%d in (*.egg-info) do @if exist "%%d" $(RMDIR) "%%d" 2>nul
	@echo → Removing .egg files...
	@for /r . %%f in (*.egg) do @if exist "%%f" $(DEL) "%%f" 2>nul
	@echo → Removing build directories...
	@if exist build $(RMDIR) build 2>nul
	@if exist dist $(RMDIR) dist 2>nul
else
	@echo "→ Removing __pycache__ directories..."
	@find . -type d -name "__pycache__" -exec $(RMDIR) {} + 2>/dev/null || true
	@echo "→ Removing .pyc files..."
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "→ Removing .pyo files..."
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@echo "→ Removing egg-info directories..."
	@find . -type d -name "*.egg-info" -exec $(RMDIR) {} + 2>/dev/null || true
	@echo "→ Removing .egg files..."
	@find . -type f -name "*.egg" -delete 2>/dev/null || true
	@echo "→ Removing build directories..."
	@$(RMDIR) build dist 2>/dev/null || true
endif
	@echo → Cleanup complete!