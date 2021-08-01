 # current 1.0.4
VERSION ?= 1.0.5
NAME ?= altmaker

REPOSITORY ?= harbor.unext.jp/datascience-dev

AWS_REPOSITORY ?= 938175221734.dkr.ecr.ap-northeast-1.amazonaws.com/datascience-dev

IMAGE ?= $(AWS_REPOSITORY)/ds-$(NAME):$(VERSION)
IMAGE_LATEST ?= $(AWS_REPOSITORY)/ds-$(NAME):latest

.EXPORT_ALL_VARIABLES:
.PHONY: build clean


build:  build_docker

build_whl:
	echo "#####build_whl#####"
	python  setup.py  sdist  bdist_wheel

build_docker:
	echo "$(IMAGE)"
	docker build -t $(IMAGE) .
	docker build -t $(IMAGE_LATEST) .
	# docker build -t $(IMAGE) -t $(IMAGE_LATEST) .

clean:
	echo "#####clean#####"
	rm -r dist build *.egg-info

push_to_harbor:
	echo $(REPOSITORY)
	docker push $(IMAGE_LATEST)
	docker push $(IMAGE)



#build: clean build_egg build_docker

#build_docker:
#    docker build -t $(IMAGE) -t $(IMAGE_LATEST) .

#build_egg:
#    pyb
#    easy_install target/dist/*/dist/*-$(VERSION).tar.gz

#push: push_to_pypicloud push_to_harbor

#push_to_pypicloud:
#    $(eval file := $(shell echo target/dist/*/dist/*-$(VERSION).tar.gz))
#    twine upload -r devhttps $(file)



#docker push content_rs:latest
