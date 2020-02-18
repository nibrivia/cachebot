install:
	sudo apt-get update && sudo apt-get install -y singularity-container
	python3 -m venv venv
	. venv/bin/activate
	pip install requests psutil
