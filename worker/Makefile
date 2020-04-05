SHELL := /bin/bash

install:
	sudo apt-get update && sudo apt-get install -y \
		python3-venv \
		python3-dev \
		build-essential \
		libssl-dev \
		uuid-dev \
		libgpgme11-dev \
		squashfs-tools \
		libseccomp-dev \
		wget \
		pkg-config \
		git
	sudo rm -rf /usr/local/go/
	export VERSION=1.12 OS=linux ARCH=amd64 && \
		wget https://dl.google.com/go/go$$VERSION.$$OS-$$ARCH.tar.gz && \
		sudo tar -C /usr/local -xzvf go$$VERSION.$$OS-$$ARCH.tar.gz && \
		rm go$$VERSION.$$OS-$$ARCH.tar.gz;
	echo 'export GOPATH=$${HOME}/go' >> ~/.bashrc && \
		echo 'export PATH=/usr/local/go/bin:$${PATH}:$${GOPATH}/bin' >> ~/.bashrc && \
		source ~/.bashrc
	export VERSION=3.3.0 && \
		export PATH=/usr/local/go/bin:$${PATH}:$${GOPATH}/bin && \
		wget https://github.com/sylabs/singularity/releases/download/v$${VERSION}/singularity-$${VERSION}.tar.gz && \
		tar -xzf singularity-$${VERSION}.tar.gz && \
		cd singularity && \
		source ~/.bashrc &&\
		./mconfig && \
		make -C ./builddir && \
		sudo make -C ./builddir install
	. /usr/local/etc/bash_completion.d/singularity
	python3 -m venv venv
	. venv/bin/activate && pip install requests psutil
	@echo "Worker installed, now run:"
	@echo "      nohup make run"

run:
	bash -c ". venv/bin/activate && git pull && python3 worker.py 2> err.log"
