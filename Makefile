install:
	sudo apt-get update && sudo apt-get install -y singularity-container python3-venv
	virtualenv venv --python=python3
	. venv/bin/activate && pip install requests psutil
	@echo "Worker installed, now run:"
	@echo "      . venv/bin/activate ; python3 worker.py"

run:
	bash -c ". venv/bin/activate ; python3 worker.py"
